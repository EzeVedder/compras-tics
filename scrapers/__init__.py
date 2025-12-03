# scrapers/__init__.py

from datetime import date as dt_date
from typing import Callable, Optional, Dict

from .boletin_tercera import scrape_boletin_tercera
from .comprar import scrape_comprar_tics          # versión HTTP original
from .comprar_bot import scrape_comprar_tics_robot  # nueva versión Selenium


# Firma estándar que espera la UI:
# (fecha_desde, fecha_hasta, carpeta_salida, callback_progreso, callback_cancelado) -> cantidad_registros
ScraperFunc = Callable[
    [dt_date, dt_date, str, Optional[Callable[[int], None]], Optional[Callable[[], bool]]],
    int,
]


# Registro de scrapers disponibles.
# La clave (string) es la que se usa en el combo del main (site_key).
SCRAPERS_REGISTRY: Dict[str, ScraperFunc] = {
    "boletin_tercera": scrape_boletin_tercera,
    "comprar_tics": scrape_comprar_tics,              # COMPR.AR vía requests
    "comprar_tics_robot": scrape_comprar_tics_robot,  # COMPR.AR vía Selenium
}


def get_scraper(site_key: str) -> ScraperFunc:
    """
    Devuelve la función scraper asociada a la clave que viene del combo del main.
    """
    try:
        return SCRAPERS_REGISTRY[site_key]
    except KeyError:
        raise ValueError(
            f"Scraper desconocido: {site_key!r}. "
            f"Claves válidas: {list(SCRAPERS_REGISTRY)}"
        )


