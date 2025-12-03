#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
subir_a_bigquery.py

Lee un JSON (lista de procesos TICS) y lo inserta en una tabla de BigQuery
usando un LOAD JOB (no streaming, compatible con free tier / sandbox).

Uso típico:

    python subir_a_bigquery.py procesos_tics.json ^
        --credentials "..\\keys\\firestore-service-account.json" ^
        --project-id "proceso-compras" ^
        --dataset "proceso_compras" ^
        --table "procesos_tics"

Requiere:
    pip install google-cloud-bigquery
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import Client as BigQueryClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sube un JSON de procesos TICS a una tabla de BigQuery."
    )
    parser.add_argument(
        "json_path",
        help="Ruta al archivo JSON de entrada (ej: procesos_tics.json)",
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="ID del proyecto de Google Cloud (ej: proceso-compras)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help=(
            "Ruta al archivo de credenciales (service account JSON). "
            "Si se omite, usará las credenciales por defecto."
        ),
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
    parser.add_argument(
        "--id-field",
        default="numero_proceso",
        help="Campo del JSON a usar como base para doc_id. Por defecto: numero_proceso",
    )
    return parser.parse_args()


def crear_cliente_bigquery(
    project_id: str,
    credentials_path: Optional[str] = None,
) -> BigQueryClient:
    if credentials_path:
        from google.oauth2 import service_account

        cred_path = Path(credentials_path)
        if not cred_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de credenciales: {cred_path}")

        print(f"[INFO] Usando credenciales: {cred_path}")
        credentials = service_account.Credentials.from_service_account_file(
            str(cred_path)
        )
        return bigquery.Client(project=project_id, credentials=credentials)
    else:
        print("[INFO] Usando credenciales por defecto.")
        return bigquery.Client(project=project_id)


def leer_json(json_path: str) -> List[Dict[str, Any]]:
    p = Path(json_path)
    if not p.exists():
        raise FileNotFoundError(f"No se encontró el archivo JSON: {p}")

    print(f"[INFO] Leyendo JSON: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("El JSON debe ser una lista de registros (array de objetos).")

    print(f"[INFO] Registros encontrados en el JSON: {len(data)}")
    return data


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


def preparar_fila(rec: Dict[str, Any], id_field: str) -> Dict[str, Any]:
    """
    Mapea el registro JSON al esquema de la tabla de BigQuery.
    """
    base_id = rec.get(id_field) or ""
    doc_id = sanitizar_doc_id(base_id) if base_id else None

    fila = {
        "doc_id": doc_id,
        "n": rec.get("n"),
        "numero_proceso": rec.get("numero_proceso"),
        "expediente": rec.get("expediente"),
        "nombre_proceso": rec.get("nombre_proceso"),
        "tipo_proceso": rec.get("tipo_proceso"),
        "fecha_apertura": rec.get("fecha_apertura"),
        "estado": rec.get("estado"),
        "unidad_ejecutora": rec.get("unidad_ejecutora"),
        "saf": rec.get("saf"),
        "detalle_productos_servicios": rec.get("detalle_productos_servicios"),
        "pliego_numero": rec.get("pliego_numero"),
        "link": rec.get("link"),
        "origen": rec.get("origen"),
        "es_tic": rec.get("es_tic"),
        "anio": rec.get("anio") or obtener_anio_desde_fecha(rec.get("fecha_apertura")),
        # String ISO, BigQuery lo castea a TIMESTAMP
        "fecha_carga": datetime.utcnow().isoformat() + "Z",
    }

    return fila


def subir_a_bigquery(
    client: BigQueryClient,
    dataset_id: str,
    table_id: str,
    registros: List[Dict[str, Any]],
    id_field: str,
) -> None:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    print(f"[INFO] Tabla destino: {table_ref}")

    filas = []
    for rec in registros:
        fila = preparar_fila(rec, id_field=id_field)
        filas.append(fila)

    print(f"[INFO] Filas a insertar: {len(filas)}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    print("[INFO] Iniciando LOAD JOB en BigQuery...")
    job = client.load_table_from_json(filas, table_ref, job_config=job_config)
    job.result()  # Esperar a que termine

    print("[OK] Load job completado sin errores.")
    tabla = client.get_table(table_ref)
    print(f"[INFO] Filas totales en la tabla ahora: {tabla.num_rows}")


def main() -> None:
    args = parse_args()

    registros = leer_json(args.json_path)
    client = crear_cliente_bigquery(
        project_id=args.project_id,
        credentials_path=args.credentials,
    )

    subir_a_bigquery(
        client=client,
        dataset_id=args.dataset,
        table_id=args.table,
        registros=registros,
        id_field=args.id_field,
    )


if __name__ == "__main__":
    main()
