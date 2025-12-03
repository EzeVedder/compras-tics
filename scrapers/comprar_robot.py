import os
import time
import re
import pandas as pd
from datetime import date as dt_date
from typing import List, Dict, Optional

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------

BASE_URL = "https://comprar.gob.ar"
COMPRAS_LIST_URL = "https://comprar.gob.ar/Compras.aspx?qs=W1HXHGHtH10="

# Palabras clave para detectar TIC
TIC_KEYWORDS = [
    "computadora", "notebook", "pc", "servidor", "server", "switch", "router",
    "firewall", "impresora", "software", "licencia", "sistema", "mantenimiento",
    "disco", "memoria", "redes", "ups", "toner", "informatica", "tecnologia"
]

def es_tic(texto: str) -> bool:
    if not texto: return False
    return any(k in texto.lower() for k in TIC_KEYWORDS)

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

# ----------------------------------------------------------------------
# Lógica del Robot
# ----------------------------------------------------------------------

def iniciar_navegador(headless=False):
    """Configura e inicia el navegador Chrome controlado por el robot."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")  # Ejecutar sin abrir ventana gráfica (más rápido)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Inicia el driver gestionando automáticamente la versión de ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def extraer_datos_detalle(html_detalle: str, url_actual: str) -> Dict:
    """Usa BeautifulSoup para parsear el HTML ya cargado por Selenium."""
    soup = BeautifulSoup(html_detalle, "html.parser")
    full_text = soup.get_text("\n")
    
    data = {
        "numero_proceso": None, "expediente": None, "objeto": None,
        "estado": None, "fecha_apertura": None, "detalle_productos": None,
        "url": url_actual
    }

    # Helper simple para buscar en texto
    lines = [l.strip() for l in full_text.splitlines() if l.strip()]
    
    def get_val(label):
        for i, line in enumerate(lines):
            if label.lower() in line.lower():
                # Intenta sacar valor de la misma linea (Label: Valor)
                if ":" in line:
                    parts = line.split(":", 1)
                    if len(parts) > 1 and parts[1].strip():
                        return parts[1].strip()
                # Si no, devuelve la siguiente línea
                if i + 1 < len(lines):
                    return lines[i+1]
        return None

    data["numero_proceso"] = get_val("Número de Procedimiento")
    data["expediente"] = get_val("Número de Expediente")
    data["objeto"] = get_val("Objeto")
    data["estado"] = get_val("Estado")
    data["fecha_apertura"] = get_val("Fecha de apertura")

    # Extracción de Renglones (Búsqueda de tabla de productos)
    # Buscamos tablas que tengan columnas numéricas
    items = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        
        # Heurística: Si la tabla tiene headers como 'Renglón' o 'Bien/Servicio'
        header_txt = rows[0].get_text().lower()
        if "renglón" in header_txt or "producto" in header_txt or "bien" in header_txt:
            for tr in rows[1:]:
                cols = [c.get_text(" ", strip=True) for c in tr.find_all(["td"])]
                if len(cols) > 1:
                    items.append(" ".join(cols))
            if items: break # Si encontramos una tabla válida, paramos
            
    data["detalle_productos"] = " | ".join(items) if items else None
    
    return data

def robot_scraper(output_file="resultado_comprar.xlsx", max_pages=1):
    driver = iniciar_navegador(headless=False) # Pon headless=True para que no se vea la ventana
    datos_totales = []

    try:
        print("🤖 Robot iniciado. Accediendo al listado...")
        driver.get(COMPRAS_LIST_URL)
        
        # Esperar a que cargue la tabla principal
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

        current_page = 1
        
        while current_page <= max_pages:
            print(f"--- Procesando Página {current_page} ---")
            
            # Re-identificar la tabla en cada vuelta (el DOM cambia)
            # Buscamos la tabla que contiene los resultados. Usualmente es la tabla grande con GridView
            rows = driver.find_elements(By.XPATH, "//table[contains(@id, 'Grid')]//tr")[1:] # Skip header
            
            # Iteramos por índice porque al volver de una pestaña, los elementos 'rows' caducan (StaleElementReference)
            num_rows = len(rows)
            print(f"Filas encontradas: {num_rows}")

            for i in range(num_rows):
                try:
                    # 1. Recuperar referencias frescas
                    table_rows = driver.find_elements(By.XPATH, "//table[contains(@id, 'Grid')]//tr")[1:]
                    if i >= len(table_rows): break
                    row = table_rows[i]
                    cols = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cols) < 5: continue

                    # Datos básicos del listado
                    nro_proceso_list = cols[0].text.strip()
                    estado_list = cols[4].text.strip()
                    
                    print(f"  > Procesando: {nro_proceso_list}")

                    # 2. ENCONTRAR Y CLICKEAR EL LINK (LA PARTE ROBOT)
                    # Buscamos el link dentro de la primera columna
                    link = cols[0].find_element(By.TAG_NAME, "a")
                    
                    # Guardamos el handle de la ventana principal
                    main_window = driver.current_window_handle
                    
                    # Hacemos Click. 
                    # Nota: A veces Javascript hace scroll, usamos execute_script para asegurar el click
                    driver.execute_script("arguments[0].click();", link)
                    
                    # 3. ESPERAR Y CAMBIAR A LA NUEVA PESTAÑA/VENTANA
                    # Esperamos a que haya una nueva ventana
                    wait.until(EC.number_of_windows_to_be(2))
                    
                    # Cambiamos el foco del robot a la nueva ventana
                    for window_handle in driver.window_handles:
                        if window_handle != main_window:
                            driver.switch_to.window(window_handle)
                            break
                    
                    # 4. SCRAPEAR EL DETALLE
                    # Esperamos a que cargue algo vital (ej. 'Estado')
                    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Estado')]")))
                    
                    # Extraemos el HTML crudo y lo pasamos a BeautifulSoup (más rápido para parsear texto)
                    html_detalle = driver.page_source
                    url_detalle = driver.current_url
                    
                    detalle_data = extraer_datos_detalle(html_detalle, url_detalle)
                    
                    # 5. CERRAR PESTAÑA Y VOLVER
                    driver.close()
                    driver.switch_to.window(main_window)
                    
                    # 6. UNIFICAR DATOS
                    registro = {
                        "nro_proceso": detalle_data["numero_proceso"] or nro_proceso_list,
                        "objeto": detalle_data["objeto"],
                        "expediente": detalle_data["expediente"],
                        "estado": detalle_data["estado"] or estado_list,
                        "fecha_apertura": detalle_data["fecha_apertura"],
                        "productos": detalle_data["detalle_productos"],
                        "link_detalle": url_detalle
                    }
                    
                    # Chequeo TIC
                    full_txt = (str(registro["objeto"]) + " " + str(registro["productos"])).lower()
                    registro["es_tic"] = es_tic(full_txt)
                    
                    datos_totales.append(registro)

                except Exception as e:
                    print(f"    [Error en fila {i}]: {e}")
                    # Asegurar volver a la ventana principal si falló algo
                    if len(driver.window_handles) > 1:
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    continue

            # --- PAGINACIÓN ---
            # Intentar ir a la siguiente página si corresponde
            if current_page < max_pages:
                try:
                    # Buscar el link de la página siguiente (usualmente '...' o el número siguiente)
                    next_page_num = str(current_page + 1)
                    # XPath complejo para encontrar el link del paginador ASP.NET
                    # Busca un <a> que contenga el texto del número o sea el botón 'siguiente'
                    next_btn = driver.find_element(By.XPATH, f"//tr[@class='pgr']//a[text()='{next_page_num}']")
                    
                    print(f"Navegando a página {next_page_num}...")
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(3) # Esperar al postback
                    current_page += 1
                except Exception as e:
                    print("No se encontró más paginación o fin de las páginas solicitadas.")
                    break
            else:
                break

    finally:
        print("🤖 Cerrando robot...")
        driver.quit()
        
        # Guardar Excel
        if datos_totales:
            df = pd.DataFrame(datos_totales)
            df.to_excel(output_file, index=False)
            print(f"✅ Archivo guardado: {output_file} con {len(df)} registros.")
        else:
            print("⚠️ No se encontraron datos.")

if __name__ == "__main__":
    # Ejecutar el robot (puedes cambiar max_pages para leer más)
    robot_scraper(output_file="compras_tic_robot.xlsx", max_pages=1)