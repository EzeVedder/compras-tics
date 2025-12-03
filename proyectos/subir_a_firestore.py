#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
subir_a_firestore.py

Lee un archivo JSON (lista de procesos) y los inserta/actualiza
en una colección de Firestore (por defecto: procesos_tics).

Uso típico:

    python subir_a_firestore.py procesos_tics.json \
        --credentials "C:\\ruta\\service-account-firestore.json" \
        --project-id "tu-project-id" \
        --collection "procesos_tics"

El JSON de entrada debe ser una lista de objetos como los que generaste con convertir_a_json.py, por ejemplo:

[
  {
    "n": 1,
    "numero_proceso": "14/1-0026-LPR25",
    "expediente": "...",
    "nombre_proceso": "...",
    "tipo_proceso": "...",
    "fecha_apertura": "18/07/2025 10:30 Hrs.",
    "estado": "Publicado",
    "unidad_ejecutora": "CNEA",
    "saf": "14",
    "detalle_productos_servicios": "...",
    "pliego_numero": "...",
    "link": "https://comprar.gob.ar/...",
    "origen": "COMPRAR"
  },
  ...
]

Cada registro se guardará como documento en la colección indicada.
El ID del documento se genera a partir de numero_proceso (sanitizado).
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.cloud import firestore
from google.cloud.firestore import Client as FirestoreClient
from google.cloud.firestore_v1 import SERVER_TIMESTAMP


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sube un JSON de procesos TICS a Firestore."
    )
    parser.add_argument(
        "json_path",
        help="Ruta al archivo JSON de entrada (ej: procesos_tics.json)",
    )
    parser.add_argument(
        "--collection",
        default="procesos_tics",
        help="Nombre de la colección de Firestore. Por defecto: procesos_tics",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="ID del proyecto de Google Cloud / Firebase. Opcional si ya está configurado por defecto.",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help=(
            "Ruta al archivo de credenciales (service account JSON). "
            "Si se omite, usará las credenciales por defecto (GOOGLE_APPLICATION_CREDENTIALS o gcloud)."
        ),
    )
    parser.add_argument(
        "--id-field",
        default="numero_proceso",
        help=(
            "Campo del JSON a usar como base para el ID del documento. "
            "Por defecto: numero_proceso."
        ),
    )
    return parser.parse_args()


def crear_cliente_firestore(
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None,
) -> FirestoreClient:
    if credentials_path:
        cred_path = Path(credentials_path)
        if not cred_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de credenciales: {cred_path}")
        print(f"[INFO] Usando credenciales: {cred_path}")
        return firestore.Client.from_service_account_json(
            str(cred_path),
            project=project_id,
        )
    else:
        print("[INFO] Usando credenciales por defecto (GOOGLE_APPLICATION_CREDENTIALS o gcloud).")
        if project_id:
            return firestore.Client(project=project_id)
        return firestore.Client()


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
    """
    Genera un ID de documento 'amigable' a partir de un valor (por ej. numero_proceso).
    Reemplaza caracteres problemáticos como '/' y espacios.
    """
    s = str(base).strip()
    # Reemplazar '/' por '-', espacios por '_'
    s = s.replace("/", "-")
    s = s.replace("\\", "-")
    s = re.sub(r"\s+", "_", s)
    # Opcional: podría bajar a minúsculas
    # s = s.lower()
    return s


def obtener_anio_desde_fecha(fecha_apertura: Any) -> Optional[int]:
    if not isinstance(fecha_apertura, str):
        return None
    # Buscar un año de 4 dígitos en el string
    m = re.search(r"(\d{4})", fecha_apertura)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def preparar_registro_para_firestore(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Permite hacer pequeñas transformaciones antes de subir a Firestore.
    - Añade campo 'anio' si se puede inferir desde fecha_apertura.
    - Añade 'fecha_carga' como SERVER_TIMESTAMP.
    """
    data = dict(rec)  # copia

    # Intentar inferir 'anio' si no está
    if "anio" not in data:
        anio = obtener_anio_desde_fecha(data.get("fecha_apertura"))
        if anio is not None:
            data["anio"] = anio

    # fecha_carga como timestamp del servidor
    data["fecha_carga"] = SERVER_TIMESTAMP

    return data


def subir_registros_a_firestore(
    db: FirestoreClient,
    collection_name: str,
    registros: List[Dict[str, Any]],
    id_field: str = "numero_proceso",
) -> None:
    col_ref = db.collection(collection_name)
    total = len(registros)

    for idx, rec in enumerate(registros, start=1):
        # Obtener valor para el ID
        base_id = rec.get(id_field)
        if base_id is None:
            # Fallback: usar índice si falta el campo
            base_id = f"doc_{idx}"

        doc_id = sanitizar_doc_id(base_id)
        data = preparar_registro_para_firestore(rec)

        print(f"[{idx}/{total}] Subiendo documento ID={doc_id} ...", end=" ")

        col_ref.document(doc_id).set(data)
        print("OK")


def main() -> None:
    args = parse_args()

    json_path = args.json_path
    collection_name = args.collection
    project_id = args.project_id
    credentials_path = args.credentials
    id_field = args.id_field

    print(f"[INFO] Colección destino: {collection_name}")
    print(f"[INFO] Campo usado como ID de documento: {id_field}")

    registros = leer_json(json_path)
    db = crear_cliente_firestore(project_id=project_id, credentials_path=credentials_path)

    subir_registros_a_firestore(
        db=db,
        collection_name=collection_name,
        registros=registros,
        id_field=id_field,
    )

    print("[OK] Todos los registros fueron subidos a Firestore.")


if __name__ == "__main__":
    main()
