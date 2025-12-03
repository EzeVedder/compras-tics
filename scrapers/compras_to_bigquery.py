#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
compras_to_bigquery.py

1) Usa comprar_bot.ejecutar_robot() para scrapear COMPRAR y armar un DataFrame.
2) Transforma las columnas al esquema de la tabla procesos_tics en BigQuery.
3) Sube los datos a BigQuery usando un LOAD JOB (compatible con free tier / sandbox).

Uso local de ejemplo:

    python compras_to_bigquery.py ^
        --credentials "C:\\ruta\\sa-key.json" ^
        --project-id "proceso-compras" ^
        --dataset "proceso_compras" ^
        --table "procesos_tics"
"""

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Client as BigQueryClient
from google.oauth2 import service_account

# Importamos la lógica de scraping desde scrapers/comprar_bot.py
from scrapers.comprar_bot import ejecutar_robot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el scraper de COMPRAR y sube el resultado a BigQuery."
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="ID del proyecto de Google Cloud (ej: proceso-compras)",
    )
    parser.add_argument(
        "--credentials",
        required=True,
        help="Ruta al archivo de credenciales (service account JSON).",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="ID del dataset de BigQuery (ej: proceso_compras)",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Nombre de la tabla de BigQuery (ej: procesos_tics)",
    )
    return parser.parse_args()


def crear_cliente_bigquery(
    project_id: str,
    credentials_path: str,
) -> BigQueryClient:
    cred_path = Path(credentials_path)
    if not cred_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de credenciales: {cred_path}")

    print(f"[INFO] Usando credenciales: {cred_path}")
    credentials = service_account.Credentials.from_service_account_file(str(cred_path))
    return bigquery.Client(project=project_id, credentials=credentials)


def sanitizar_doc_id(base: Any) -> str:
    s = str(base).strip()
    s = s.replace("/", "-").replace("\\", "-")
    s = re.sub(r"\s+", "_", s)
    return s


def obtener_anio_desde_fecha(fecha_apertura: Any) -> Optional[int]:
    if not isinstance(fecha_apertura, str):
        return None
    m = re.search(r"(\d{4})", fecha_apertura)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def df_a_registros_bigquery(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Toma el DataFrame devuelto por ejecutar_robot() y lo mapea
    al esquema de la tabla procesos_tics en BigQuery.
    """
    registros: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        numero_proceso = row.get("numero_proceso")
        doc_id = sanitizar_doc_id(numero_proceso) if numero_proceso else None

        fecha_apertura = row.get("fecha_apertura")
        anio = obtener_anio_desde_fecha(fecha_apertura)

        registro = {
            "doc_id": doc_id,
            "n": None,  # si querés, se puede reemplazar por un contador incremental
            "numero_proceso": numero_proceso,
            "expediente": row.get("expediente"),
            "nombre_proceso": row.get("nombre_proceso"),
            "tipo_proceso": row.get("tipo_proceso"),
            "fecha_apertura": fecha_apertura,
            "estado": row.get("estado"),
            "unidad_ejecutora": row.get("unidad_ejecutora"),
            "saf": row.get("saf"),
            "detalle_productos_servicios": row.get("detalle_productos"),
            "pliego_numero": row.get("pliego_nombre"),
            "link": row.get("url_detalle") or row.get("pliego_url"),
            "origen": "COMPRAR",
            "es_tic": True,
            "anio": anio,
            "fecha_carga": datetime.utcnow().isoformat() + "Z",
        }
        registros.append(registro)

    return registros


def subir_a_bigquery(
    client: BigQueryClient,
    dataset_id: str,
    table_id: str,
    registros: List[Dict[str, Any]],
) -> None:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    print(f"[INFO] Tabla destino: {table_ref}")
    print(f"[INFO] Filas a insertar: {len(registros)}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    print("[INFO] Iniciando LOAD JOB en BigQuery...")
    job = client.load_table_from_json(registros, table_ref, job_config=job_config)
    job.result()

    print("[OK] Load job completado sin errores.")
    tabla = client.get_table(table_ref)
    print(f"[INFO] Filas totales en la tabla ahora: {tabla.num_rows}")


def main() -> None:
    args = parse_args()

    # 1) Ejecutar el robot de COMPRAR
    print("[INFO] Ejecutando robot de COMPRAR...")
    df = ejecutar_robot()

    if df is None or df.empty:
        print("[WARN] El DataFrame devuelto por ejecutar_robot() está vacío. No se insertan datos.")
        return

    print(f"[INFO] Procesos obtenidos: {len(df)}")
    print(f"[INFO] Columnas del DataFrame: {list(df.columns)}")

    # 2) Mapear DataFrame al esquema de BigQuery
    registros = df_a_registros_bigquery(df)

    if not registros:
        print("[WARN] No hay registros para insertar en BigQuery.")
        return

    # 3) Crear cliente de BigQuery
    client = crear_cliente_bigquery(
        project_id=args.project_id,
        credentials_path=args.credentials,
    )

    # 4) Subir a BigQuery
    subir_a_bigquery(
        client=client,
        dataset_id=args.dataset,
        table_id=args.table,
        registros=registros,
    )


if __name__ == "__main__":
    main()
