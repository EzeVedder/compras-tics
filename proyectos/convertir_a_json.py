#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
convertir_a_json.py

Convierte una hoja de Excel a un archivo JSON.

Uso básico:
    python convertir_a_json.py input.xlsx

Uso con opciones:
    python convertir_a_json.py input.xlsx \
        --sheet "Hoja1 (2)" \
        --header-row 4 \
        --output procesos_tics.json \
        --modelo-tics

Notas:
- --header-row se indica en base 1 (1 = primera fila de Excel, 4 = encabezados en fila 4, etc.).
- Si usás --modelo-tics, renombra las columnas del Excel al modelo de Firestore para procesos_tics.
"""

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convierte una hoja de Excel a un JSON (lista de registros)."
    )
    parser.add_argument(
        "excel_path",
        help="Ruta al archivo Excel de entrada (ej: Copia de Compras APN - TICS 2025.xlsx)",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Nombre de la hoja a leer. Si se omite, usa la hoja por defecto del archivo.",
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=1,
        help=(
            "Número de fila (1-based) donde están los encabezados de columna. "
            "Ej: si los encabezados están en la fila 4 de Excel, usar --header-row 4. "
            "Por defecto: 1 (primera fila)."
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help=(
            "Ruta del archivo JSON de salida. "
            "Si se omite, usa el mismo nombre del Excel con extensión .json."
        ),
    )
    parser.add_argument(
        "--modelo-tics",
        action="store_true",
        help=(
            "Aplica el renombre de columnas al modelo de procesos_tics "
            "(n, numero_proceso, expediente, nombre_proceso, etc.)."
        ),
    )
    return parser.parse_args()


def get_default_output_path(excel_path: str) -> Path:
    p = Path(excel_path)
    return p.with_suffix(".json")


def cargar_excel_a_dataframe(
    excel_path: str, sheet_name: str | None, header_row_1_based: int
) -> pd.DataFrame:
    header_idx = header_row_1_based - 1  # pandas usa 0-based
    df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        header=header_idx,
        engine="openpyxl",
    )
    # Eliminar filas completamente vacías
    df = df.dropna(how="all")
    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how="all")
    return df


def aplicar_modelo_tics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas al modelo de Firestore para procesos_tics.

    Mapeo esperado desde el Excel (ejemplo basado en tu archivo):
        "N°" -> "n"
        "Número proceso" -> "numero_proceso"
        "Expediente" -> "expediente"
        "Nombre proceso" -> "nombre_proceso"
        "Tipo de Proceso" -> "tipo_proceso"
        "Fecha de apertura" -> "fecha_apertura"
        "Estado" -> "estado"
        "Unidad Ejecutora" -> "unidad_ejecutora"
        "Servicio Administrativo Financiero" -> "saf"
        "Detalle de productos o servicios" -> "detalle_productos_servicios"
        "Pliego N°" -> "pliego_numero"
        "LINK" -> "link"
        "BORA/COMPRAR" -> "origen"
    """
    rename_map = {
        "N°": "n",
        "Nº": "n",  # por si cambia el símbolo
        "Número proceso": "numero_proceso",
        "Numero proceso": "numero_proceso",
        "Expediente": "expediente",
        "Nombre proceso": "nombre_proceso",
        "Tipo de Proceso": "tipo_proceso",
        "Tipo de proceso": "tipo_proceso",
        "Fecha de apertura": "fecha_apertura",
        "Estado": "estado",
        "Unidad Ejecutora": "unidad_ejecutora",
        "Servicio Administrativo Financiero": "saf",
        "Detalle de productos o servicios": "detalle_productos_servicios",
        "Pliego N°": "pliego_numero",
        "Pliego No": "pliego_numero",
        "LINK": "link",
        "BORA/COMPRAR": "origen",
    }

    # Solo renombra las columnas que existan en el DataFrame
    columnas_presentes = {col: rename_map[col] for col in df.columns if col in rename_map}
    df = df.rename(columns=columnas_presentes)

    return df


def dataframe_a_json(df: pd.DataFrame, output_path: Path) -> None:
    # Reemplazar NaN por None para que sea JSON válido
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()

    excel_path = args.excel_path
    sheet_name = args.sheet
    header_row = args.header_row
    output_path = Path(args.output) if args.output else get_default_output_path(excel_path)

    print(f"[INFO] Leyendo Excel: {excel_path}")
    if sheet_name:
        print(f"[INFO] Hoja: {sheet_name}")
    print(f"[INFO] Fila de encabezados (1-based): {header_row}")

    df = cargar_excel_a_dataframe(excel_path, sheet_name, header_row)

    print(f"[INFO] Columnas originales: {list(df.columns)}")

    if args.modelo_tics:
        df = aplicar_modelo_tics(df)
        print(f"[INFO] Columnas después de aplicar modelo_tics: {list(df.columns)}")

    print(f"[INFO] Filas a exportar: {len(df)}")

    dataframe_a_json(df, output_path)

    print(f"[OK] JSON generado en: {output_path.resolve()}")


if __name__ == "__main__":
    main()
