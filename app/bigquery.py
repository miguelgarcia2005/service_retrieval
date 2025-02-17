from google.cloud import bigquery
import os
from dotenv import load_dotenv
from vertexai.language_models import TextEmbeddingModel

# Cargar variables de entorno
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

# Inicializar el cliente de BigQuery y el modelo de embeddings
bq_client = bigquery.Client()
embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")

def insertar_chunks_en_bigquery(documento, parrafos, intencion, subintencion):
    """Inserta los chunks extraídos en BigQuery, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    rows = []

    for i, parrafo in enumerate(parrafos):
        # Generar el embedding para el párrafo (chunk)
        embedding = embedding_model.get_embeddings([parrafo])[0].values
        # Construir el registro con el embedding incluido
        row = {
            "id": f"{intencion}_{subintencion}_{documento}_chunk_{i}",
            "name_document": documento,
            "chunk_id": i,
            "text": parrafo,
            "intent": intencion,
            "sub_intent": subintencion,
            "is_transactional": 'N',
            "embedding_value": embedding
        }
        print(f'##### El valor del embedding es: {embedding} ####\n\n')
        rows.append(row)

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")
