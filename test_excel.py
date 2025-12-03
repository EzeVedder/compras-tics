from datetime import date
from scrapers.comprar_bot import scrape_comprar_tics_selenium

hoy = date.today()
scrape_comprar_tics_selenium(hoy, hoy, r"C:\ruta\de\salida")  # cambiá la ruta
