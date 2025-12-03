import os
import re
from datetime import date as dt_date
from typing import Dict, Optional, List, Callable

import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse, parse_qs
import unicodedata


# ----------------------------------------------------------------------
# Configuración básica
# ----------------------------------------------------------------------

BASE_URL = "https://comprar.gob.ar"
# Endpoint de "Ver todos" (Procesos de compra)
COMPRAS_LIST_URL = "https://comprar.gob.ar/Compras.aspx?qs=W1HXHGHtH10="

# Encabezados básicos para que el sitio nos trate como navegador "normal"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}
# Palabras clave para marcar procesos TIC (más estrictas)
# Palabras clave para marcar procesos TIC (más estrictas)
TIC_KEYWORDS = [
    # 🖥️ Equipamiento informático / hardware
    "computadora",
    "computadoras",
    "notebook",
    "notebooks",
    "laptop",
    "laptops",
    # Ojo: NO usamos "pc" solo, para no confundir con "10 pc" como unidades
    "pc de escritorio",
    "equipo informático",
    "equipo informatico",
    "equipos informáticos",
    "equipos informaticos",
    "equipamiento informático",
    "equipamiento informatico",
    "servidor",
    "servidores",
    "impresora",
    "impresoras",
    "multifunción",
    "multifuncion",
    "plotter",
    "scanner",
    "escáner",
    "monitor",
    "monitores",
    "teclado",
    "teclados",
    "mouse",
    "mouses",
    "disco rígido",
    "discos rígidos",
    "disco duro",
    "discos duros",
    "ssd",

    # 🌐 Redes y comunicaciones
    "switch",
    "switches",
    "router",
    "routers",
    "firewall",
    "firewalls",
    "access point",
    "punto de acceso",
    "wifi",
    "redes informáticas",
    "redes informaticas",
    "cableado estructurado",

    # 🏢 Infraestructura / datacenter
    "datacenter",
    "data center",
    "virtualización",
    "virtualizacion",
    "storage",
    "cabina de almacenamiento",
    "infraestructura tecnológica",
    "infraestructura tecnologica",

    # 💿 Software y licencias (cuando es claramente de software)
    "software",
    "sistema informático",
    "sistema informatico",
    "sistemas informáticos",
    "sistemas informaticos",
    "programa informático",
    "programa informatico",
    "programas informáticos",
    "programas informaticos",
    "licencia de software",
    "licencias de software",
    "antivirus",
    "licencias de uso de software",
    "suscripción de software",
    "suscripcion de software",

    # 🛠️ Desarrollo y mantenimiento de software
    "desarrollo de software",
    "desarrollo de sistemas",
    "desarrollo informático",
    "desarrollo informatico",
    "mantenimiento de software",
    "mantenimiento de sistemas",
    "implementación de software",
    "implementacion de software",

    # 🧩 Servicios TIC
    "servicios informáticos",
    "servicios informaticos",
    "servicios de informática",
    "servicios de informatica",
    "soporte técnico informático",
    "soporte tecnico informatico",
    "servicio técnico informático",
    "servicio tecnico informatico",

    # ☁️ Cloud / hosting
    "hosting",
    "cloud",
    "nube",
    "saas",
    "iaas",
    "paas",

    # 🎯 Frases paraguas TIC (sin la palabra "tic" suelta)
    "tecnologías de la información",
    "tecnologias de la informacion",
]



# ----------------------------------------------------------------------
# Helpers generales
# ----------------------------------------------------------------------

def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _strip_accents(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# ----------------------------------------------------------------------
# Helpers de la página de detalle
# ----------------------------------------------------------------------

def _extract_lines(soup: BeautifulSoup) -> List[str]:
    """Devuelve todo el texto de la página como lista de líneas limpias."""
    full_text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in full_text.splitlines()]
    return [ln for ln in lines if ln]


def _extract_renglones_from_text(lines: List[str], debug: bool = False) -> Optional[str]:
    """
    Extrae el bloque de 'Detalle de productos o servicios' (o similares)
    usando SOLO el texto plano de la página (sin depender de la estructura <table>).

    Devuelve todas las líneas de ese bloque concatenadas en un único string.
    """
    header_idx: Optional[int] = None

    # Buscar la cabecera del bloque de renglones
    for idx, line in enumerate(lines):
        low = line.lower()
        if (
            "detalle de productos o servicios" in low
            or "detalle de bienes y servicios" in low
            or "detalle de bienes o servicios" in low
            or "renglones de la convocatoria" in low
        ):
            header_idx = idx
            if debug:
                print(f"[_extract_renglones_from_text] Header encontrado en idx={idx}: {line!r}")
            break

    if header_idx is None:
        if debug:
            print("[_extract_renglones_from_text] No se encontró encabezado de renglones.")
        return None

    detalle_lines: List[str] = []
    for line in lines[header_idx + 1:]:
        text = line.strip()
        if not text:
            continue

        low = text.lower()

        # Fin del bloque: cuando empieza otra sección
        if text.startswith("#### "):
            if debug:
                print(f"[_extract_renglones_from_text] Corte por nueva sección: {text!r}")
            break

        # En muchas páginas, luego de la tabla aparece una 'x' de cierre de modal
        if text == "×":
            if debug:
                print("[_extract_renglones_from_text] Corte por '×' (fin de bloque de detalle).")
            break

        detalle_lines.append(text)

    if not detalle_lines:
        if debug:
            print("[_extract_renglones_from_text] Header encontrado pero sin líneas de detalle.")
        return None

    if debug:
        print(f"[_extract_renglones_from_text] Total líneas de detalle capturadas: {len(detalle_lines)}")

    # Unimos todo en un solo string
    return " | ".join(detalle_lines)





def _find_after_label(lines: List[str], label: str, max_lookahead: int = 6) -> Optional[str]:
    """
    Busca una línea que coincide con el label y devuelve la primera no vacía siguiente.
    """
    pattern = re.compile(rf"^{re.escape(label)}$", re.I)
    for idx, line in enumerate(lines):
        if pattern.match(line):
            for j in range(idx + 1, min(idx + 1 + max_lookahead, len(lines))):
                candidate = lines[j].strip()
                if not candidate:
                    continue
                if candidate.startswith("####"):
                    break
                return candidate
    return None


def _find_colon_value(lines: List[str], label: str) -> Optional[str]:
    """
    Busca líneas del tipo 'Label: Valor' y devuelve 'Valor'.
    """
    regex = re.compile(rf"{re.escape(label)}\s*:\s*(.+)", re.I)
    for line in lines:
        m = regex.search(line)
        if m:
            return m.group(1).strip()
    return None




def extract_renglones(soup: BeautifulSoup, debug: bool = False) -> Optional[str]:
    """
    Wrapper que usa el texto plano de la página para extraer el bloque
    de renglones (detalle de productos o servicios).

    Se apoya en _extract_renglones_from_text y en _extract_lines.
    """
    lines = _extract_lines(soup)
    if debug:
        print(f"[extract_renglones] Cantidad de líneas de texto en la página: {len(lines)}")
    return _extract_renglones_from_text(lines, debug=debug)





def get_renglones_from_pliego(pliego_url: str) -> Optional[str]:
    """
    Si el detalle de productos no está en la página del proceso,
    entra al pliego (VistaPreviaPliegoCiudadano.aspx) y obtiene los renglones
    usando la lógica basada en texto.
    """
    if not pliego_url:
        return None

    print(f"[get_renglones_from_pliego] Consultando pliego: {pliego_url}")

    try:
        resp = requests.get(pliego_url, headers=DEFAULT_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[get_renglones_from_pliego] ERROR al descargar pliego: {e}")
        return None

    content_type = resp.headers.get("Content-Type", "").lower()
    if "pdf" in content_type or "octet-stream" in content_type:
        print("[get_renglones_from_pliego] El pliego es un PDF, no se pueden leer renglones.")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    detalle = extract_renglones(soup, debug=True)

    if not detalle:
        print("[get_renglones_from_pliego] No se pudieron extraer renglones del pliego.")
    return detalle




def _extract_renglones_block(soup: BeautifulSoup, lines: List[str]) -> Optional[str]:
    """
    Extrae el bloque de renglones (detalle de bienes/servicios).
    1) Intenta por títulos tipo "Renglones de la convocatoria".
    2) Si falla, busca tablas con encabezados típicos y concatena todas las filas.
    """
    # 1) Intento por texto "Renglones ..."
    header_idx: Optional[int] = None
    for i, line in enumerate(lines):
        if re.search(r"Renglones\s+de\s+la\s+convocatoria", line, re.I) or \
           re.search(r"Renglones\s+Convocatoria", line, re.I) or \
           re.search(r"Detalle\s+de\s+bienes\s+y\s+servicios", line, re.I):
            header_idx = i
            break

    if header_idx is not None:
        start = header_idx + 1
        detalle_lines: List[str] = []
        for line in lines[start:]:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            if line_stripped.startswith("####"):
                break
            detalle_lines.append(line_stripped)
        if detalle_lines:
            # Todas las líneas (incluyendo varios renglones) en un solo string
            return " | ".join(detalle_lines)

    # 2) Fallback por tabla con encabezados típicos
    candidate_tables = []
    for table in soup.find_all("table"):
        header_tr = table.find("tr")
        if not header_tr:
            continue
        headers = [
            _clean_text(c.get_text(" ", strip=True)).lower()
            for c in header_tr.find_all(["th", "td"])
        ]
        header_str = " ".join(headers)
        if any(
            kw in header_str
            for kw in [
                "número de renglón",
                "numero de renglón",
                "numero de renglon",
                "objeto del gasto",
                "descripción del bien",
                "descripcion del bien",
                "detalle del bien",
                "detalle del producto",
            ]
        ):
            candidate_tables.append(table)

    if not candidate_tables:
        return None

    desc_parts: List[str] = []
    for table in candidate_tables:
        rows = table.find_all("tr")[1:]  # salteamos encabezado
        for tr in rows:
            cols = [
                _clean_text(c.get_text(" ", strip=True))
                for c in tr.find_all(["td", "th"])
            ]
            cols = [c for c in cols if c]
            if not cols:
                continue
            # Cada renglón -> "col1 | col2 | col3 ..."
            desc_parts.append(" | ".join(cols))

    if not desc_parts:
        return None

    # Todos los renglones juntos, separados por '; '
    return "; ".join(desc_parts)


def _extract_pliego_info(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    """
    Busca en la sección de Anexos (o tabla similar) el pliego:
    - Prioriza filas donde algún texto contenga "Pliego".
    - Si no, toma el primer link que haya como fallback.
    """
    pliego_nombre: Optional[str] = None
    pliego_url: Optional[str] = None

    # Primero, tratamos de ubicar la sección Anexos
    header = soup.find(string=re.compile(r"Anexos", re.I))
    table = None
    if header:
        header_tag = header.find_parent()
        if header_tag:
            table = header_tag.find_next("table")

    # Si no encontramos por "Anexos", buscamos una tabla con headers típicos
    if not table:
        for tbl in soup.find_all("table"):
            header_tr = tbl.find("tr")
            if not header_tr:
                continue
            headers = [
                _clean_text(c.get_text(" ", strip=True)).lower()
                for c in header_tr.find_all(["th", "td"])
            ]
            header_str = " ".join(headers)
            if "nombre" in header_str and ("tipo" in header_str or "anexo" in header_str):
                table = tbl
                break

    if not table:
        return {"pliego_nombre": None, "pliego_url": None}

    def is_pliego_text(text: str) -> bool:
        t = _strip_accents(text).lower()
        return "pliego" in t

    best_row = None
    best_href = None

    # 1) Buscamos filas cuyo nombre contenga "pliego"
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all(["td", "th"])
        if not cols:
            continue
        nombre_txt = _clean_text(cols[0].get_text(" ", strip=True))
        a = tr.find("a", href=True)
        href = a["href"] if a else None
        if nombre_txt and is_pliego_text(nombre_txt) and href:
            best_row = nombre_txt
            best_href = href
            break

    # 2) Si no encontramos por nombre, buscamos por texto "pliego" en cualquier columna
    if not best_href:
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all(["td", "th"])
            if not cols:
                continue
            text_all = " ".join(
                _clean_text(c.get_text(" ", strip=True)) for c in cols
            )
            a = tr.find("a", href=True)
            href = a["href"] if a else None
            if text_all and is_pliego_text(text_all) and href:
                best_row = _clean_text(cols[0].get_text(" ", strip=True))
                best_href = href
                break

    # 3) Fallback: primer link que haya en la tabla
    if not best_href:
        for tr in table.find_all("tr")[1:]:
            a = tr.find("a", href=True)
            if a:
                href = a["href"]
                nombre_txt = _clean_text(a.get_text(" ", strip=True))
                best_row = nombre_txt or None
                best_href = href
                break

    if best_href:
        if best_href.startswith("http"):
            pliego_url = best_href
        else:
            pliego_url = urljoin(BASE_URL, best_href)
    pliego_nombre = best_row

    return {"pliego_nombre": pliego_nombre, "pliego_url": pliego_url}



def _extract_detalle_from_pliego(pliego_url: str) -> Optional[str]:
    """
    Descarga la página de VistaPrevia del pliego y extrae el detalle
    de renglones usando extract_renglones (nueva lógica).
    """
    if not pliego_url:
        return None

    try:
        resp = requests.get(pliego_url, headers=DEFAULT_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[COMPR.AR] No se pudo descargar el pliego {pliego_url}: {exc}")
        return None

    # Si el pliego es un PDF u otro binario, no podemos parsear renglones
    content_type = resp.headers.get("Content-Type", "").lower()
    if "pdf" in content_type or "octet-stream" in content_type:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    return extract_renglones(soup)



def normalize_convocatoria_url(href: str) -> Optional[str]:
    """
    Normaliza el href del listado a una URL de detalle usable.

    - Si es un javascript:window.open('...'), extrae la URL interna.
    - Si es una URL relativa, la convierte en absoluta contra BASE_URL.
    - Ignora sólo los javascript: que NO contienen ninguna URL adentro.
    """
    if not href:
        return None

    href = href.strip()
    import re
    from urllib.parse import urljoin

    # Caso 1: href = "javascript:window.open('/PLIEGO/....aspx?qs=...', ...)"
    if href.lower().startswith("javascript:"):
        # 1a) Buscar primero un http(s) completo dentro del javascript
        m = re.search(r"(https?://[^'\";]+)", href, flags=re.IGNORECASE)
        if m:
            return m.group(1)

        # 1b) Buscar una ruta tipo /PLIEGO/VistaPrevia.... dentro de comillas
        m = re.search(
            r"['\"](\/?PLIEGO\/VistaPrevia[^'\";]+)['\"]",
            href,
            flags=re.IGNORECASE,
        )
        if m:
            inner = m.group(1)
            return urljoin(BASE_URL, inner)

        # 1c) Fallback genérico: cualquier cosa con "VistaPrevia"
        m = re.search(
            r"['\"](\/?[^'\";]*VistaPrevia[^'\";]+)['\"]",
            href,
            flags=re.IGNORECASE,
        )
        if m:
            inner = m.group(1)
            return urljoin(BASE_URL, inner)

        # Si no encontramos nada parecido a URL adentro del javascript, no sirve
        return None

    # Caso 2: href normal (no javascript)
    # Si ya viene con esquema http/https, lo devolvemos tal cual
    if href.startswith("http://") or href.startswith("https://"):
        return href

    # Caso 3: ruta relativa: la normalizamos contra BASE_URL
    href = href.lstrip("~")  # por si viene "~/PLIEGO/..."
    return urljoin(BASE_URL, href)



def fetch_convocatoria_html(url: str, session: Optional[requests.Session] = None) -> str:
    sess = session or requests.Session()
    resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_convocatoria_fields(html: str, url: str) -> Dict[str, Optional[str]]:
    """
    Extrae campos clave de la Vista Previa de una convocatoria.

    Si el detalle de renglones no aparece en la VistaPrevia principal,
    intenta obtenerlo desde el pliego (VistaPreviaPliegoCiudadano.aspx)
    usando el link identificado en la sección de Anexos.
    """
    soup = BeautifulSoup(html, "html.parser")
    lines = _extract_lines(soup)

    numero_expediente = _find_after_label(lines, "Número de Expediente")
    numero_proceso = _find_after_label(lines, "Número de Procedimiento")
    tipo_proceso = _find_after_label(lines, "Tipo de Procedimiento")
    nombre_proceso = _find_after_label(lines, "Objeto")

    estado = _find_colon_value(lines, "Estado")
    fecha_apertura = _find_colon_value(lines, "Fecha de apertura")

    uoc = _find_after_label(lines, "Unidad Operativa de Contrataciones")
    saf = _find_after_label(lines, "Servicio Administrativo Financiero")

    # Extraemos la info del pliego (nombre + URL) desde la sección de anexos
    pliego_info = _extract_pliego_info(soup)
    pliego_url = pliego_info.get("pliego_url")

    # ---- DEBUG ----
    print(f"[extract_convocatoria_fields] URL detalle = {url}")
    print(f"  -> numero_proceso={numero_proceso} | expediente={numero_expediente}")
    print(f"  -> pliego_url={pliego_url}")
    # ---------------

    # Primero intentamos extraer los renglones desde la VistaPrevia principal
    # (poné debug=False si no querés el detalle por fila acá)
    detalle_productos = extract_renglones(soup, debug=False)

    if detalle_productos:
        print(f"[extract_convocatoria_fields]   Renglones en VistaPrevia (len={len(detalle_productos)})")
    else:
        print("[extract_convocatoria_fields]   SIN renglones en VistaPrevia.")

    # Si no hay renglones, intentar obtenerlos desde el pliego
    if (not detalle_productos) and pliego_url:
        print("[extract_convocatoria_fields]   Buscando renglones en pliego...")
        detalle_desde_pliego = get_renglones_from_pliego(pliego_url)
        if detalle_desde_pliego:
            detalle_productos = detalle_desde_pliego
            print(f"[extract_convocatoria_fields]   Renglones en pliego (len={len(detalle_productos)})")
        else:
            print("[extract_convocatoria_fields]   Tampoco se encontraron renglones en pliego.")

    if not detalle_productos:
        print("[extract_convocatoria_fields]   >>> detalle_productos = None")

    return {
        "numero_proceso": numero_proceso,
        "expediente": numero_expediente,
        "nombre_proceso": nombre_proceso,
        "tipo_proceso": tipo_proceso,
        "fecha_apertura": fecha_apertura,
        "estado": estado,
        "unidad_ejecutora": uoc,
        "saf": saf,
        "detalle_productos": detalle_productos,
        "pliego_nombre": pliego_info.get("pliego_nombre"),
        "pliego_url": pliego_url,
        "url": url,
    }



def scrape_convocatoria_detail(
    input_href: str, session: Optional[requests.Session] = None
) -> Dict[str, Optional[str]]:
    """
    Recibe el href del listado, resuelve la URL real de detalle,
    entra a la Vista Previa y devuelve los campos parseados.
    """
    url = normalize_convocatoria_url(input_href)
    if not url:
        raise ValueError(f"No se pudo resolver URL de detalle a partir de: {input_href!r}")
    html = fetch_convocatoria_html(url, session=session)
    data = extract_convocatoria_fields(html, url=url)
    return data


# ----------------------------------------------------------------------
# Helpers de la página de listado (Compras.aspx?qs=...)
# ----------------------------------------------------------------------

def _collect_form_state(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Recolecta inputs hidden para poder hacer postbacks ASP.NET (si hace falta).
    """
    form_state: Dict[str, str] = {}
    form = soup.find("form")
    if not form:
        return form_state
    for inp in form.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if not name:
            continue
        value = inp.get("value", "")
        form_state[name] = value
    return form_state


def _parse_postback_from_href(href: Optional[str]) -> Optional[Dict[str, str]]:
    """
    Dado un href tipo:
      javascript:__doPostBack('ctl00$CPH1$GridListaPliegosAperturaProxima$ctl03$lnkNumeroProceso','')
    devuelve:
      {
        "event_target": "ctl00$CPH1$GridListaPliegosAperturaProxima$ctl03$lnkNumeroProceso",
        "event_argument": ""
      }
    """
    if not href:
        return None
    href = href.strip()
    m = re.search(r"__doPostBack\('([^']*)','([^']*)'\)", href)
    if not m:
        return None
    return {
        "event_target": m.group(1),
        "event_argument": m.group(2),
    }


def fetch_detalle_proceso_via_postback(
    numero_proceso: str,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Optional[str]]]:
    """
    Simula hacer clic en un número de proceso dentro de Compras.aspx.
    Envía un POST con __EVENTTARGET/__EVENTARGUMENT y todo el estado del formulario.
    Devuelve los campos parseados del detalle.
    """
    import urllib.parse

    if session is None:
        session = requests.Session()

    resp = session.get(COMPRAS_LIST_URL, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = _find_grid_table(soup)
    if not table:
        print("[fetch_detalle_proceso_via_postback] No se encontró la tabla.")
        return None

    postback_info = None
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        nro = _clean_text(tds[0].get_text(" ", strip=True))
        if nro == numero_proceso:
            link = tr.find("a", href=True)
            if not link:
                print(f"[fetch_detalle_proceso_via_postback] Fila {numero_proceso} sin link.")
                return None
            postback_info = _parse_postback_from_href(link["href"])
            print(f"[fetch_detalle_proceso_via_postback] Postback info: {postback_info}")
            break

    if not postback_info:
        print(f"[fetch_detalle_proceso_via_postback] No se encontró el proceso {numero_proceso}.")
        return None

    form_state = _collect_form_state(soup)
    data = {
        "__EVENTTARGET": postback_info["event_target"],
        "__EVENTARGUMENT": postback_info["event_argument"],
        "__LASTFOCUS": "",
    }
    data.update(form_state)

    # ASP.NET espera application/x-www-form-urlencoded (no multipart)
    payload = urllib.parse.urlencode(data, safe="$()")

    headers = DEFAULT_HEADERS.copy()
    headers["Content-Type"] = "application/x-www-form-urlencoded"

    r = session.post(COMPRAS_LIST_URL, data=payload, headers=headers, timeout=30)
    r.raise_for_status()

    html = r.text
    if "GridListaPliegosAperturaProxima" in html:
        print("[fetch_detalle_proceso_via_postback] ⚠️ Seguimos en el listado, el postback no abrió el detalle.")
        return None

    print(f"[fetch_detalle_proceso_via_postback] ✔️ Longitud HTML detalle: {len(html)}")
    return extract_convocatoria_fields(html, url=r.url)



def _parse_total_results(soup: BeautifulSoup) -> Optional[int]:
    """
    Intenta leer el mensaje 'Se han encontrado (N) resultados'.
    """
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Se han encontrado\s*\((\d+)\)\s*resultados", text)
    if m:
        return int(m.group(1))
    return None


def _find_grid_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    Ubica la tabla principal de resultados del listado.
    """
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [
            _clean_text(c.get_text(" ", strip=True))
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


def _extract_list_rows_from_soup(soup: BeautifulSoup) -> List[Dict[str, Optional[str]]]:
    """
    Extrae una lista de dicts con los datos de cada fila del listado.
    Además intenta obtener el href de detalle de cada proceso.
    """
    table = _find_grid_table(soup)
    if not table:
        return []

    rows_data: List[Dict[str, Optional[str]]] = []
    body_rows = table.find_all("tr")[1:]  # salteamos encabezado

    debug_count = 0  # para no spamear la consola

    for tr in body_rows:
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        numero_proceso = _clean_text(cols[0].get_text(" ", strip=True))
        if not numero_proceso:
            continue

        # --- BUSCAMOS EL LINK DE DETALLE EN TODA LA FILA ---
        link = tr.find("a", href=True)
        detalle_href = link.get("href") if link else None
        detalle_url = normalize_convocatoria_url(detalle_href) if detalle_href else None

        if debug_count < 5:
            print(
                f"[_extract_list_rows_from_soup] nro_proc={numero_proceso} | "
                f"raw_href={detalle_href} | detalle_url={detalle_url}"
            )
            debug_count += 1
        # ---------------------------------------------------

        nombre_proceso = _clean_text(cols[1].get_text(" ", strip=True)) if len(cols) > 1 else None
        tipo_proceso = _clean_text(cols[2].get_text(" ", strip=True)) if len(cols) > 2 else None
        fecha_apertura = _clean_text(cols[3].get_text(" ", strip=True)) if len(cols) > 3 else None
        estado = _clean_text(cols[4].get_text(" ", strip=True)) if len(cols) > 4 else None
        unidad_ejecutora = _clean_text(cols[5].get_text(" ", strip=True)) if len(cols) > 5 else None
        saf = _clean_text(cols[6].get_text(" ", strip=True)) if len(cols) > 6 else None

        rows_data.append(
            {
                "numero_proceso_list": numero_proceso,
                "nombre_proceso_list": nombre_proceso,
                "tipo_proceso_list": tipo_proceso,
                "fecha_apertura_list": fecha_apertura,
                "estado_list": estado,
                "unidad_ejecutora_list": unidad_ejecutora,
                "saf_list": saf,
                "detalle_url": detalle_url,
            }
        )

    return rows_data



def _parse_pager_target(soup: BeautifulSoup) -> Optional[str]:
    """
    Para el caso ASP.NET clásico: busca __doPostBack('GRID','Page$2')
    y devuelve el nombre del control GRID (EVENTTARGET).
    """
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"__doPostBack\('([^']+)',\s*'Page\$\d+'\)", href)
        if m:
            return m.group(1)
    return None


def _parse_simple_pager_links(soup: BeautifulSoup) -> List[str]:
    """
    Fallback: busca links directos a otras páginas de Compras.aspx?...
    donde el texto del link sea '2', '3', etc.
    """
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").strip()
        if not txt.isdigit():
            continue
        href = a["href"]
        if "Compras.aspx" in href:
            links.append(urljoin(BASE_URL, href))
    seen = set()
    unique_links: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            unique_links.append(u)
    return unique_links


def _iter_compras_pages(session: requests.Session, max_pages: Optional[int] = None) -> List[BeautifulSoup]:
    """
    Devuelve una lista de soups, uno por cada página del listado.
    Maneja dos casos:
      - Paginación por links simples (?page=2, etc.)
      - Paginación por __doPostBack (ASP.NET clásico).
    """
    soups: List[BeautifulSoup] = []

    # Página 1
    resp = session.get(COMPRAS_LIST_URL, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    soups.append(soup)

    total_results = _parse_total_results(soup)
    first_rows = _extract_list_rows_from_soup(soup)
    page_size = len(first_rows)

    total_pages: Optional[int] = None
    if total_results is not None and page_size:
        total_pages = (total_results + page_size - 1) // page_size

    # Intento 1: links simples
    simple_links = _parse_simple_pager_links(soup)
    if simple_links:
        for idx, url in enumerate(simple_links, start=2):
            if max_pages is not None and idx > max_pages:
                break
            r = session.get(url, headers=DEFAULT_HEADERS, timeout=30)
            r.raise_for_status()
            soups.append(BeautifulSoup(r.text, "html.parser"))
        return soups

    # Intento 2: __doPostBack
    pager_target = _parse_pager_target(soup)
    if not pager_target or not total_pages or total_pages <= 1:
        return soups

    current_soup = soup
    for page in range(2, total_pages + 1):
        if max_pages is not None and page > max_pages:
            break

        form_data = _collect_form_state(current_soup)
        form_data["__EVENTTARGET"] = pager_target
        form_data["__EVENTARGUMENT"] = f"Page${page}"

        r = session.post(
            COMPRAS_LIST_URL,
            data=form_data,
            headers=DEFAULT_HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        current_soup = BeautifulSoup(r.text, "html.parser")
        soups.append(current_soup)

    return soups


# ----------------------------------------------------------------------
# Clasificación TIC (solo marca, no filtra)
# ----------------------------------------------------------------------

def es_tic(texto: Optional[str]) -> bool:
    """Marca si el texto parece describir una compra TIC."""
    if not texto:
        return False
    txt = _strip_accents(texto).lower()

    for kw in TIC_KEYWORDS:
        kw_norm = _strip_accents(kw).lower()
        # Por si en el futuro se cuela alguna keyword muy corta
        if len(kw_norm) <= 2:
            continue
        if kw_norm in txt:
            return True
    return False


# ----------------------------------------------------------------------
# Orquestador principal para la UI (usa la firma estándar)
# ----------------------------------------------------------------------

def scrape_comprar_tics(
    start_date: dt_date,
    end_date: dt_date,
    output_dir: str,
    progress_callback: Optional[Callable[[int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> int:
    """
    Scrapea TODOS los procesos del listado 'Ver todos' y entra al detalle
    de cada proceso para completar campos. NO filtra por TIC; agrega
    una columna es_tic para que vos lo filtres después.

    Además, cuando el detalle de renglones no está en la VistaPrevia
    principal, intenta completarlo a partir del pliego (pliego_url),
    concatenando todas las líneas (renglones) que encuentre.
    """
    os.makedirs(output_dir, exist_ok=True)

    session = requests.Session()

    # 1) Traemos todas las páginas del listado
    list_soups = _iter_compras_pages(session)
    list_rows: List[Dict[str, Optional[str]]] = []
    for sp in list_soups:
        list_rows.extend(_extract_list_rows_from_soup(sp))

    total = len(list_rows)
    if total == 0:
        if progress_callback:
            progress_callback(100)
        return 0

    # 2) Entramos al detalle de cada proceso y mergeamos datos
    records: List[Dict[str, Optional[str]]] = []

    for idx, row in enumerate(list_rows, start=1):
        if is_cancelled and is_cancelled():
            break

        detalle_url = row.get("detalle_url")
        detail_data: Dict[str, Optional[str]] = {}

        if detalle_url:
            print(f"[scrape_comprar_tics] ({idx}/{total}) Detalle: {detalle_url}")
            try:
                detail_data = scrape_convocatoria_detail(detalle_url, session=session)
            except Exception as exc:
                print(f"[COMPR.AR] Error al scrapear detalle {detalle_url}: {exc}")
                detail_data = {}
        else:
            numero_list = row.get("numero_proceso_list")
            print(
                f"[scrape_comprar_tics] ({idx}/{total}) "
                f"Sin detalle_url, probando postback para {numero_list}..."
            )
            if numero_list:
                try:
                    detail_data = fetch_detalle_proceso_via_postback(
                        numero_list,
                        session=session,
                    ) or {}
                except Exception as exc:
                    print(
                        f"[COMPR.AR] Error al scrapear detalle vía postback "
                        f"({numero_list}): {exc}"
                    )
                    detail_data = {}
            else:
                detail_data = {}


        # Si no pudimos entrar al detalle, igual guardamos lo que tengamos del listado
        merged: Dict[str, Optional[str]] = {
            "numero_proceso": detail_data.get("numero_proceso") or row.get("numero_proceso_list"),
            "expediente": detail_data.get("expediente"),
            "nombre_proceso": detail_data.get("nombre_proceso") or row.get("nombre_proceso_list"),
            "tipo_proceso": detail_data.get("tipo_proceso") or row.get("tipo_proceso_list"),
            "fecha_apertura": detail_data.get("fecha_apertura") or row.get("fecha_apertura_list"),
            "estado": detail_data.get("estado") or row.get("estado_list"),
            "unidad_ejecutora": detail_data.get("unidad_ejecutora") or row.get("unidad_ejecutora_list"),
            "saf": detail_data.get("saf") or row.get("saf_list"),
            "detalle_productos": detail_data.get("detalle_productos"),
            "pliego_nombre": detail_data.get("pliego_nombre"),
            "pliego_url": detail_data.get("pliego_url"),
            "url_detalle": detail_data.get("url") or detalle_url,
        }

        # ---- DEBUG: ver si el valor viene cargado acá ----
        if idx <= 10:  # mostramos sólo los primeros 10 para no explotar la consola
            dp = merged.get("detalle_productos")
            print(
                f"[scrape_comprar_tics]   numero_proceso={merged.get('numero_proceso')}, "
                f"detalle_productos={'OK' if dp else 'VACIO'}, "
                f"len={len(dp) if isinstance(dp, str) else 0}"
            )
        # ---------------------------------------------------

        # Marcamos si parece TIC (pero NO filtramos)
        texto_tic = " ".join(
            t
            for t in [
                merged.get("detalle_productos") or "",
                merged.get("nombre_proceso") or "",
            ]
            if t
        )
        merged["es_tic"] = es_tic(texto_tic)

        records.append(merged)

        if progress_callback:
            progress = int(idx * 100 / total)
            progress_callback(progress)

    if not records:
        if progress_callback:
            progress_callback(100)
        return 0

    df = pd.DataFrame(records)

    # El rango de fechas se usa sólo para el nombre del archivo (para ser consistente con el resto de la app)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    filename = f"comprar_tics_{start_str}_{end_str}.xlsx"
    output_path = os.path.join(output_dir, filename)



    df = pd.DataFrame(records)

    # ---- DEBUG: ver qué quedó en la columna detalle_productos ----
    try:
        print("[scrape_comprar_tics] Vista previa de detalle_productos en DataFrame:")
        print(df[["numero_proceso", "detalle_productos"]].head(10).to_string())
    except Exception as exc:
        print(f"[scrape_comprar_tics] No se pudo mostrar vista previa: {exc}")
    # --------------------------------------------------------------

    # El rango de fechas se usa sólo para el nombre del archivo (para ser consistente con el resto de la app)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    filename = f"comprar_tics_{start_str}_{end_str}.xlsx"
    output_path = os.path.join(output_dir, filename)


    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"[COMPR.AR] Exportado {len(df)} procesos a '{output_path}'")

    if progress_callback:
        progress_callback(100)







    return len(df)




if __name__ == "__main__":
    # Test rápido de un solo proceso usando postback
    from pprint import pprint

    nro_test = "38/19-1191-LPR25"
    print(f"Probando fetch_detalle_proceso_via_postback({nro_test!r})...")
    datos = fetch_detalle_proceso_via_postback(nro_test)
    pprint(datos)