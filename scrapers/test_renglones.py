from bs4 import BeautifulSoup
import requests
from comprar import extract_renglones

# URL de prueba: reemplazá por un caso real con varios renglones
URL = "https://comprar.gob.ar/PLIEGO/VistaPreviaPliegoCiudadano.aspx?qs=BQoBkoMoEhynFFnpQdifivLxaWgV3BzmboXRA4jovcdKmlhjyEQagYTGPXbr5WAGzM|08IpeuWKiROqbRvO61PRVJ6MgmH6MpzGnBeGGlF9XOaEq6TYCchVjaroLga6h5Q5E|Oqa4xY="

print(f"Probando extracción de renglones en:\n{URL}\n")

try:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
except Exception as e:
    print(f"❌ Error al descargar: {e}")
    exit()

soup = BeautifulSoup(resp.text, "html.parser")

# Mostrar títulos de todas las tablas
print("Tablas encontradas:")
for i, table in enumerate(soup.find_all("table")):
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    print(f"- Tabla {i+1}: {headers}")

# Intentar extraer renglones
detalle = extract_renglones(soup)

print("\nResultado de extract_renglones:")
print(detalle if detalle else "❌ No se encontró ningún detalle de productos.")
