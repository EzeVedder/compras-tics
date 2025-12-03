# scrapers/boletin_tercera.py

import os
import re
import time
from datetime import date as dt_date, timedelta
from typing import Optional, Callable

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.boletinoficial.gob.ar"
REQUEST_DELAY = 1.0  # pausa entre requests a cada aviso, en segundos


# ----------------------------------------------------------------------
# Listado de avisos por fecha
# ----------------------------------------------------------------------
def _get_listado_avisos(fecha: dt_date):
    """
    Descarga la página de la Tercera Sección para una fecha dada
    y devuelve una lista de avisos con título y URL de detalle.

    URL usada:
    https://www.boletinoficial.gob.ar/seccion/tercera/YYYYMMDD
    """
    fecha_str_path = fecha.strftime("%Y%m%d")
    section_url = f"{BASE_URL}/seccion/tercera/{fecha_str_path}"

    try:
        resp = requests.get(section_url, timeout=20)
    except Exception as e:
        print(f"   ⚠ Error de conexión para {fecha}: {e!r}")
        return []

    if resp.status_code != 200:
        print(
            f"   ⚠ No se pudo obtener la sección para {fecha} "
            f"(status {resp.status_code})"
        )
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    avisos = []
    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/detalleAviso/tercera/" in href:
            titulo = " ".join(a.get_text(strip=True).split())
            url_completa = href if href.startswith("http") else BASE_URL + href

            avisos.append(
                {
                    "titulo_listado": titulo,
                    "url": url_completa,
                    "fecha_edicion": fecha.isoformat(),
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


# ----------------------------------------------------------------------
# Resumen solo del Objeto / Asunto
# ----------------------------------------------------------------------
def _extraer_resumen_objeto(texto: str) -> Optional[str]:
    """
    Recibe un texto grande (párrafo completo / bloque) y devuelve
    solo el texto del Objeto/Asunto (sin plazos, retiro de pliego, etc.).
    """
    if not texto:
        return None

    texto = " ".join(texto.split())

    # Buscamos "Objeto" o "Asunto" (soporta variantes y acentos)
    patron = re.compile(
        r"(Objeto(?: de la contrataci[oó]n)?|Objeto de la licitaci[oó]n|Asunto)\s*:?",
        re.IGNORECASE,
    )
    m = patron.search(texto)
    if not m:
        return None

    # Lo que viene después de "Objeto ... : / Asunto :"
    sub = texto[m.end():].strip()

    # Cortes típicos donde termina la descripción del Objeto
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
        "Fecha de publicación",
        "Compartir por email",
    ]

    corte_idx = len(sub)
    lower_sub = sub.lower()
    for palabra in cortes:
        pos = lower_sub.find(palabra.lower())
        if pos != -1 and pos < corte_idx:
            corte_idx = pos

    resumen = sub[:corte_idx].strip(" .-;:")
    return resumen or None


# ----------------------------------------------------------------------
# Parser de un aviso puntual
# ----------------------------------------------------------------------
def _parse_aviso(url: str) -> dict:
    """
    Dado el URL de un aviso, entra al detalle y extrae campos clave:
    - organismo (H1)
    - proceso (H2)
    - fecha_publicacion
    - resumen_proyecto: descripción COMPLETA (bloque entre H2 y "Fecha de publicación")
    - objeto_resumen: solo el texto del Objeto/Asunto (si existe)
    - url
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Organismo (primer H1 de contenido)
    h1 = soup.find("h1")
    organismo = h1.get_text(strip=True) if h1 else None

    # Proceso (primer H2 después del H1)
    if h1 is not None:
        h2 = h1.find_next("h2")
    else:
        h2 = soup.find("h2")

    proceso = h2.get_text(strip=True) if h2 else None

    # Lista con TODO el texto de la página, en orden
    strings = list(soup.stripped_strings)

    # ---------------- Bloque resumen_proyecto ----------------
    descripcion_completa: Optional[str] = None

    if proceso and proceso in strings:
        idx_proc = strings.index(proceso)
        desc_parts = []
        for s in strings[idx_proc + 1 :]:
            # Cortamos cuando aparece "Fecha de publicación" o "Compartir por email"
            if "Fecha de publicación" in s or "Compartir por email" in s:
                break
            desc_parts.append(s)

        if desc_parts:
            descripcion_completa = " ".join(desc_parts).strip()

    # Fallback: si por alguna razón no encontramos el proceso en strings,
    # probamos con el primer bloque <p>/<div> después del H2
    if descripcion_completa is None and h2 is not None:
        bloque = h2.find_next(["p", "div"])
        if bloque:
            descripcion_completa = " ".join(bloque.stripped_strings)

    # ---------------- Objeto / Asunto (objeto_resumen) ----------------
    objeto_resumen: Optional[str] = None
    if descripcion_completa:
        objeto_resumen = _extraer_resumen_objeto(descripcion_completa)

    # ---------------- Fecha de publicación ----------------
    fecha_pub = None
    for node in soup.find_all(string=lambda s: s and "Fecha de publicación" in s):
        texto = node.parent.get_text(" ", strip=True)
        fecha_pub = texto.replace("Fecha de publicación", "").strip()
        break

    return {
        "organismo": organismo,
        "proceso": proceso,
        "fecha_publicacion": fecha_pub,
        # 👇 Bloque completo, desde después del título de la licitación
        #    hasta antes de "Fecha de publicación" / "Compartir por email"
        "resumen_proyecto": descripcion_completa,
        # 👇 Solo el texto del Objeto / Asunto, si lo hay
        "objeto_resumen": objeto_resumen,
        "url": url,
    }


# ----------------------------------------------------------------------
# Scraper principal Boletín Oficial - Tercera Sección
# ----------------------------------------------------------------------
def scrape_boletin_tercera(
    start_date: dt_date,
    end_date: dt_date,
    output_dir: str,
    progress_callback: Optional[Callable[[int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> int:
    """
    Scrapea la sección tercera del Boletín Oficial en el rango [start_date, end_date]
    y exporta un Excel en output_dir.

    Devuelve la cantidad de registros exportados.
    """
    if end_date < start_date:
        raise ValueError("end_date no puede ser menor que start_date")

    registros = []

    total_days = (end_date - start_date).days + 1
    if total_days <= 0:
        total_days = 1

    fecha_actual = start_date
    day_index = 0  # 0..total_days-1

    while fecha_actual <= end_date:
        # Chequeo de cancelación a nivel día
        if is_cancelled and is_cancelled():
            print("Scraping cancelado por el usuario (por fecha).")
            break

        print(f"\n=== Boletín Tercera - edición {fecha_actual} ===")
        avisos = _get_listado_avisos(fecha_actual)
        n_av = len(avisos)
        print(f"   {n_av} avisos encontrados para {fecha_actual}.")

        if n_av == 0:
            # Día sin avisos: actualizamos progreso y seguimos
            day_index += 1
            if progress_callback:
                frac = day_index / total_days
                pct = min(100, max(0, int(frac * 100)))
                progress_callback(pct)
            fecha_actual += timedelta(days=1)
            continue

        for idx_aviso, aviso in enumerate(avisos, start=1):
            # Chequeo de cancelación a nivel aviso
            if is_cancelled and is_cancelled():
                print("Scraping cancelado por el usuario (en avisos).")
                break

            print(f"   [{idx_aviso}/{n_av}] Procesando: {aviso['titulo_listado']}")
            try:
                data = _parse_aviso(aviso["url"])
            except Exception as e:
                print(f"      ⚠ Error al procesar {aviso['url']}: {e!r}")
                continue

            data["titulo_listado"] = aviso["titulo_listado"]
            data["fecha_edicion"] = aviso["fecha_edicion"]
            registros.append(data)

            # Pequeña pausa para no golpear el sitio
            time.sleep(REQUEST_DELAY)

            # Progreso fino: día + aviso dentro del día
            if progress_callback:
                frac = (day_index + (idx_aviso / n_av)) / total_days
                pct = min(100, max(0, int(frac * 100)))
                progress_callback(pct)

        # Si se canceló dentro del loop de avisos, salimos del while principal
        if is_cancelled and is_cancelled():
            break

        day_index += 1
        fecha_actual += timedelta(days=1)

    # ---------------- Export ----------------
    df = pd.DataFrame(registros)
    print("\nPrimeras filas:")
    print(df.head())

    if df.empty:
        print("\n⚠ Atención: el DataFrame está vacío, no se exporta nada.")
        return 0

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    filename = f"contrataciones_tercera_{start_str}_{end_str}.xlsx"
    output_file = os.path.join(output_dir, filename)

    try:
        df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"\n✅ Archivo '{output_file}' generado.")
    except Exception as e:
        print("\n❌ Error al intentar escribir el Excel:")
        print(repr(e))
        raise

    return len(df)
