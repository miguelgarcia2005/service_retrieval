from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

bq_client = bigquery.Client()

def insertar_chunks_en_bigquery(documento, parrafos, intencion, subintencion):
    """Inserta los chunks extraídos en BigQuery"""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    rows = [
        {
            "id": f"{intencion}_{subintencion}_{documento}_chunk_{i}",
            "documento": documento,
            "chunk_id": i,
            "texto": parrafo,
            "intencion": intencion,
            "subintencion": subintencion
        }
        for i, parrafo in enumerate(parrafos)
    ]

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")
