import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Optional

BASE_URL = "https://www.boletinoficial.gob.ar"
SECTION_URL = f"{BASE_URL}/seccion/tercera"

# Pequeña pausa entre requests para no pegarle tan fuerte al sitio (en segundos)
REQUEST_DELAY = 1.0


def get_listado_avisos():
    """
    Descarga la página de la Tercera Sección y devuelve
    una lista de avisos con título y URL de detalle.
    """
    resp = requests.get(SECTION_URL, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    avisos = []
    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Nos quedamos solo con los links a detalle de la tercera sección
        if "/detalleAviso/tercera/" in href:
            titulo = " ".join(a.get_text(strip=True).split())
            url_completa = href if href.startswith("http") else BASE_URL + href

            avisos.append(
                {
                    "titulo_listado": titulo,
                    "url": url_completa,
                }
            )

    # Eliminamos duplicados por URL
    vistos = set()
    avisos_unicos = []
    for av in avisos:
        if av["url"] in vistos:
            continue
        vistos.add(av["url"])
        avisos_unicos.append(av)

    return avisos_unicos


def extraer_resumen_desde_detalle(texto: str) -> Optional[str]:
    """
    Recibe el bloque de texto donde está Objeto/Asunto, plazos, etc.,
    y devuelve únicamente el resumen del proyecto (después de 'Objeto:' o 'ASUNTO:').
    """
    if not texto:
        return None

    # Normalizamos espacios
    texto = " ".join(texto.split())

    # Posibles etiquetas que introducen el resumen
    claves = [
        "Objeto:",
        "OBJETO:",
        "Objeto de la contratación:",
        "Objeto de la contratacion:",
        "Objeto de la licitación:",
        "Objeto de la licitacion:",
        "ASUNTO:",
        "Asunto:",
    ]

    idx = -1
    clave_encontrada = None
    for clave in claves:
        pos = texto.find(clave)
        if pos != -1 and (idx == -1 or pos < idx):
            idx = pos
            clave_encontrada = clave

    if idx == -1:
        return None

    # Nos quedamos con lo que viene después de la etiqueta encontrada
    sub = texto[idx + len(clave_encontrada):].strip()

    # Cortamos cuando empiezan otras secciones típicas del aviso
    cortes = [
        "Retiro del Pliego",
        "Retiro del pliego",
        "Presentación de Ofertas",
        "Presentacion de Ofertas",
        "Consulta del Pliego",
        "Plazo y horario",
        "Plazo y Horario",
        "VALOR DEL PLIEGO",
        "Valor del Pliego",
        "DIRECCION INSTITUCIONAL DE CORREO ELECTRONICO",
        "Dirección institucional de correo electrónico",
        "LUGAR DE CONSULTAS",
        "Lugar de consultas",
        "FECHA Y HORA ACTO DE APERTURA",
        "Fecha y hora acto de apertura",
    ]

    corte_idx = len(sub)
    for palabra in cortes:
        pos = sub.find(palabra)
        if pos != -1 and pos < corte_idx:
            corte_idx = pos

    resumen = sub[:corte_idx].strip(" .-;:")
    return resumen or None


def parse_aviso(url: str) -> dict:
    """
    Dado el URL de un aviso de la Tercera Sección, entra al detalle y extrae:
    - organismo (H1)
    - proceso / tipo de licitación (H2)
    - fecha de publicación
    - texto_detalle (renglón donde está Objeto/Asunto, plazos, etc.)
    - resumen_proyecto (solo la parte de Objeto/Asunto)
    """
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Organismo y proceso
    h1 = soup.find("h1")
    h2 = soup.find("h2")

    organismo = h1.get_text(strip=True) if h1 else None
    proceso = h2.get_text(strip=True) if h2 else None

    texto_detalle = None
    resumen = None

    # 🔎 Buscamos específicamente un texto que contenga Objeto o Asunto
    objeto_node = soup.find(
        string=lambda s: s
        and (
            "Objeto:" in s
            or "OBJETO:" in s
            or "ASUNTO:" in s
            or "Asunto:" in s
        )
    )

    if objeto_node:
        # Tomamos todo el texto del padre (incluye expediente, objeto/asunto, plazos, etc.)
        texto_detalle = objeto_node.parent.get_text(" ", strip=True)
        resumen = extraer_resumen_desde_detalle(texto_detalle)

    # Fallback: si por alguna razón no encontró nada, usamos el bloque después de H2
    if texto_detalle is None and h2:
        bloque = h2.find_next(["p", "div"])
        if bloque:
            texto_detalle = " ".join(bloque.stripped_strings)
            if resumen is None:
                resumen = extraer_resumen_desde_detalle(texto_detalle)

    # Buscamos la línea que contiene "Fecha de publicación"
    fecha_pub = None
    for node in soup.find_all(string=lambda s: s and "Fecha de publicación" in s):
        texto = node.parent.get_text(" ", strip=True)
        # Ej: "Fecha de publicación 18/11/2025"
        fecha_pub = texto.replace("Fecha de publicación", "").strip()
        break

    return {
        "organismo": organismo,
        "proceso": proceso,
        "fecha_publicacion": fecha_pub,
        "resumen_proyecto": resumen,
        "texto_detalle": texto_detalle,
        "url": url,
    }


def main():
    # 1) Listamos avisos del día en la Tercera sección
    avisos = get_listado_avisos()
    print(f"Se encontraron {len(avisos)} avisos en la sección Tercera.")

    registros = []

    # 2) Recorremos TODOS los avisos
    for i, aviso in enumerate(avisos, start=1):
        print(f"[{i}/{len(avisos)}] Procesando: {aviso['titulo_listado']}")
        try:
            data = parse_aviso(aviso["url"])
        except Exception as e:
            print(f"   ⚠ Error al procesar {aviso['url']}: {e!r}")
            continue

        data["titulo_listado"] = aviso["titulo_listado"]
        registros.append(data)

        # Pausa entre requests para ser amables con el servidor
        time.sleep(REQUEST_DELAY)

    # 3) Pasamos a DataFrame
    df = pd.DataFrame(registros)
    print("\nPrimeras filas:")
    print(df.head())

    # 4) Guardamos a Excel
    output_file = "contrataciones_tercera.xlsx"

    if df.empty:
        print("\n⚠ Atención: el DataFrame está vacío, no se exporta nada.")
        return

    try:
        df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"\n✅ Archivo '{output_file}' generado en esta carpeta.")
    except Exception as e:
        print("\n❌ Error al intentar escribir el Excel:")
        print(repr(e))


if __name__ == "__main__":
    main()
