# scrapers/comprar_bot.py
import os
import time
from datetime import date as dt_date
from typing import List, Dict, Optional, Callable

import re 
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager

# Importamos helpers desde comprar.py
try:
    # Modo paquete: python -m scrapers.comprar_bot
    from .comprar import (
        extract_convocatoria_fields,
        es_tic,
        _extract_lines,
        _find_after_label,
    )
except ImportError:
    # Modo script dentro de la carpeta scrapers
    from comprar import (
        extract_convocatoria_fields,
        es_tic,
        _extract_lines,
        _find_after_label,
    )



BASE_URL = "https://comprar.gob.ar"
DEFAULT_URL = f"{BASE_URL}/Default.aspx"


# ----------------------------------------------------------------------
# Driver Selenium
# ----------------------------------------------------------------------
def crear_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )
    return driver


# ----------------------------------------------------------------------
# Navegación al listado "Ver todos"
# ----------------------------------------------------------------------
def ir_a_listado(driver: webdriver.Chrome, timeout: int = 20) -> None:
    print("🤖 Robot iniciado. Accediendo al listado...")
    driver.get(DEFAULT_URL)
    wait = WebDriverWait(driver, timeout)

    # Click en "Ver todos"
    ver_todos = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(normalize-space(.), 'Ver todos')]")
        )
    )
    ver_todos.click()

    # Esperamos a que aparezca la tabla principal
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//table[.//th[contains(., 'Número de Proceso')]]")
        )
    )
    time.sleep(0.5)


# ----------------------------------------------------------------------
# Parsing del listado (usamos BeautifulSoup)
# ----------------------------------------------------------------------
def _find_grid_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [
            c.get_text(" ", strip=True)
            for c in header_row.find_all(["th", "td"])
        ]
        header_text = " ".join(headers)
        if (
            "Número de Proceso" in header_text
            and "Nombre descriptivo de Proceso" in header_text
            and "Fecha de Apertura" in header_text
        ):
            return table
    return None


def obtener_filas_listado(html: str) -> List[Dict[str, Optional[str]]]:
    """
    Devuelve una lista con lo que trae la grilla de listado:
    - Número de proceso
    - Nombre descriptivo (Objeto)
    - Tipo de Proceso
    - Fecha de apertura
    - Estado
    - Unidad Ejecutora
    - SAF
    """
    soup = BeautifulSoup(html, "html.parser")
    table = _find_grid_table(soup)
    if not table:
        return []

    filas: List[Dict[str, Optional[str]]] = []
    body_rows = table.find_all("tr")[1:]  # salteamos encabezado

    for tr in body_rows:
        cols = tr.find_all("td")
        if len(cols) < 4:
            continue

        numero = cols[0].get_text(" ", strip=True)
        if not numero:
            continue


        objeto = cols[1].get_text(" ", strip=True) if len(cols) > 1 else None
        tipo = cols[2].get_text(" ", strip=True) if len(cols) > 2 else None
        fecha_apertura = cols[3].get_text(" ", strip=True) if len(cols) > 3 else None
        estado = cols[4].get_text(" ", strip=True) if len(cols) > 4 else None
        unidad_ejecutora = cols[5].get_text(" ", strip=True) if len(cols) > 5 else None
        saf = cols[6].get_text(" ", strip=True) if len(cols) > 6 else None

        # 🔎 Filtro para evitar la fila del paginador (solo números / espacios)
        # Si el texto NO tiene ninguna letra, lo descartamos
        if not any(ch.isalpha() for ch in numero):
            continue

        filas.append(
            {
                "numero_proceso": numero,
                "objeto": objeto,
                "tipo": tipo,
                "fecha_apertura": fecha_apertura,
                "estado": estado,
                "unidad_ejecutora": unidad_ejecutora,
                "saf": saf,
            }
        )

    return filas


# ----------------------------------------------------------------------
# Detalle de un proceso (click + parseo)
# ----------------------------------------------------------------------
def scrapear_detalle_proceso(
    driver: webdriver.Chrome,
    numero_proceso: str,
    timeout: int = 10,
) -> Optional[Dict[str, Optional[str]]]:
    """
    Busca el link con el texto 'numero_proceso', hace click con JS,
    espera cambio de URL, parsea con extract_convocatoria_fields
    y vuelve al listado.

    Además:
    - Fuerza que el Pliego N° sea el 'Número GDE' encontrado en el detalle,
      si existe.
    """
    wait = WebDriverWait(driver, timeout)
    old_url = driver.current_url

    try:
        link = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, numero_proceso))
        )
    except TimeoutException:
        print(f"    [Error] No se encontró link clickeable para {numero_proceso}")
        return None

    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", link)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", link)
    except WebDriverException as e:
        print(f"    [Error] No se pudo hacer click en {numero_proceso}: {e}")
        return None

    # Esperamos cambio de URL
    try:
        wait.until(EC.url_changes(old_url))
    except TimeoutException:
        print(f"    [Aviso] No cambió la URL para {numero_proceso}, continúo igual.")

    # Esperamos que haya <body>
    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        pass

    time.sleep(0.5)
    detalle_html = driver.page_source
    url_detalle = driver.current_url

    # Parseo base (usa anexos para pliego, etc.)
    data = extract_convocatoria_fields(detalle_html, url=url_detalle)

    #    el "Pliego N°" debe salir de "Número GDE" en el detalle.
    # 🔎 Ajuste: buscar correctamente el "Número GDE" del pliego
    try:
        soup = BeautifulSoup(detalle_html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # 1️⃣ Buscamos patrón explícito PLIEG-XXXX
        import re
        m = re.search(r"(PLIEG-\d{4,}-[A-Z0-9#\-]+)", text)
        numero_gde = m.group(1) if m else None

        # 2️⃣ Si no se encontró, probamos con el método de líneas
        if not numero_gde:
            lines = _extract_lines(soup)
            numero_gde = _find_after_label(lines, "Número GDE")

    except Exception:
        numero_gde = None

    if numero_gde:
        # Sobrescribimos el pliego_nombre con el Número GDE real
        data["pliego_nombre"] = numero_gde


    # Volver al listado
    driver.back()

    # Esperamos volver al listado (por URL o por tabla)
    try:
        wait.until(EC.url_to_be(old_url))
    except TimeoutException:
        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//table[.//th[contains(., 'Número de Proceso')]]")
                )
            )
        except TimeoutException:
            pass

    time.sleep(0.3)
    return data


# ----------------------------------------------------------------------
# Paginación
# ----------------------------------------------------------------------
def ir_a_pagina(driver: webdriver.Chrome, nro_pagina: int, timeout: int = 10) -> bool:
    """
    Intenta ir a la página nro_pagina haciendo click en el link de paginación.
    Devuelve True si se pudo, False si ya no hay más páginas.
    """
    wait = WebDriverWait(driver, timeout)
    try:
        link = driver.find_element(By.LINK_TEXT, str(nro_pagina))
    except NoSuchElementException:
        return False

    try:
        driver.execute_script("arguments[0].click();", link)
    except WebDriverException:
        try:
            link.click()
        except WebDriverException as e:
            print(f"   [Paginación] Error al ir a página {nro_pagina}: {e}")
            return False

    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//table[.//th[contains(., 'Número de Proceso')]]")
            )
        )
        time.sleep(0.5)
    except TimeoutException:
        print(f"   [Paginación] Timeout esperando recargar página {nro_pagina}")
        return False

    return True


# ----------------------------------------------------------------------
# Núcleo del robot: recorre listado + detalle y devuelve lista de dicts
# ----------------------------------------------------------------------
def ejecutar_robot(
    max_paginas: Optional[int] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> pd.DataFrame:
    """
    Recorre el listado completo "Ver todos", entra a cada proceso y
    devuelve un DataFrame con la info fusionada (listado + detalle).
    """
    driver = crear_driver(headless=True)
    registros: List[Dict[str, Optional[str]]] = []

    try:
        ir_a_listado(driver)
        pagina = 1
        total_procesos = 0

        while True:
            print(f"--- Procesando Página {pagina} ---")
            html = driver.page_source
            filas = obtener_filas_listado(html)
            print(f"Filas encontradas: {len(filas)}")
            if not filas:
                break

            for idx, fila in enumerate(filas):
                if is_cancelled and is_cancelled():
                    print("[COMPR.AR ROBOT] Cancelado por el usuario.")
                    return pd.DataFrame(registros) if registros else pd.DataFrame()

                numero = fila["numero_proceso"]
                print(f"  > Procesando: {numero}")

                try:
                    detalle = scrapear_detalle_proceso(driver, numero)
                except Exception as e:
                    print(f"    [Error en fila {idx}]: {e}")
                    detalle = None

                if not detalle:
                    continue

                # 💡 Merge listado + detalle
                #    - Nombre proceso: detalle.nombre_proceso o 'objeto' del listado
                #    - Estado y SAF: prioridad al listado (como pediste)
                # Armar registro base
                registro: Dict[str, Optional[str]] = {
                    "numero_proceso": detalle.get("numero_proceso") or numero,
                    "expediente": detalle.get("expediente"),
                    "nombre_proceso": detalle.get("nombre_proceso") or fila.get("objeto"),
                    "tipo_proceso": detalle.get("tipo_proceso") or fila.get("tipo"),
                    "fecha_apertura": detalle.get("fecha_apertura") or fila.get("fecha_apertura"),
                    "estado": fila.get("estado") or detalle.get("estado"),
                    "unidad_ejecutora": fila.get("unidad_ejecutora") or detalle.get("unidad_ejecutora"),
                    "saf": fila.get("saf") or detalle.get("saf"),
                    "detalle_productos": detalle.get("detalle_productos"),
                    "pliego_nombre": detalle.get("pliego_nombre"),
                    "pliego_url": detalle.get("pliego_url"),
                    "url_detalle": detalle.get("url"),
                }

                # Clasificación TIC / no TIC en base a nombre + detalle
                texto_tic = " ".join(
                    t
                    for t in [
                        registro.get("nombre_proceso") or "",
                        registro.get("detalle_productos") or "",
                    ]
                    if t
                )
                registro["es_tic"] = es_tic(texto_tic)

                registros.append(registro)
                total_procesos += 1



                if progress_callback:
                    progress = min(total_procesos, 99)
                    progress_callback(progress)

            pagina += 1
            if max_paginas is not None and pagina > max_paginas:
                break
            if not ir_a_pagina(driver, pagina):
                break

    finally:
        print("🤖 Cerrando robot...")
        try:
            driver.quit()
        except Exception:
            pass

    if not registros:
        print("⚠️ No se encontraron datos.")
        return pd.DataFrame()

    return pd.DataFrame(registros)


# ----------------------------------------------------------------------
# Función con firma estándar para la UI
# ----------------------------------------------------------------------
def scrape_comprar_tics_robot(
    start_date: dt_date,
    end_date: dt_date,
    output_dir: str,
    progress_callback: Optional[Callable[[int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> int:
    """
    Wrapper compatible con la UI:
    (fecha_desde, fecha_hasta, carpeta_salida, progress_callback, is_cancelled) -> cantidad_registros
    """
    os.makedirs(output_dir, exist_ok=True)

    df = ejecutar_robot(
        #max_paginas=None,
        max_paginas=1,
        progress_callback=progress_callback,
        is_cancelled=is_cancelled,
    )
    if df.empty:
        if progress_callback:
            progress_callback(100)
        return 0

    # Renombrar columnas y ordenar según lo que necesitás
    columnas_export = {
        "numero_proceso": "Número proceso",
        "expediente": "Expediente",
        "nombre_proceso": "Nombre proceso",
        "tipo_proceso": "Tipo de Proceso",
        "fecha_apertura": "Fecha de apertura",
        "estado": "Estado",
        "unidad_ejecutora": "Unidad Ejecutora",
        "saf": "Servicio Administrativo Financiero",
        "detalle_productos": "Detalle de productos o servicios",
        "pliego_nombre": "Pliego N°",
        "url_detalle": "LINK",
        "es_tic": "Es TIC"
    }

    df = df.rename(columns=columnas_export)
    df = df[list(columnas_export.values())]

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    output_file = os.path.join(output_dir, f"comprar_selenium_{start_str}_{end_str}.xlsx")

    df.to_excel(output_file, index=False, engine="openpyxl")
    print(f"✅ Exportado {len(df)} procesos a {output_file}")

    if progress_callback:
        progress_callback(100)

    return len(df)


if __name__ == "__main__":
    # Prueba rápida en modo script
    hoy = dt_date.today()
    scrape_comprar_tics_robot(hoy, hoy, output_dir=".")
