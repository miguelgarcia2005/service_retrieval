from google.cloud import bigquery
import os
from dotenv import load_dotenv
from vertexai.language_models import TextEmbeddingModel
import vertexai

# ==============================
# Cargar variables de entorno
# ==============================
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TABLE_ID_BETA = os.getenv("TABLE_ID_BETA")

# ==============================
# Inicializar Vertex AI y BigQuery
# ==============================
vertexai.init(project=PROJECT_ID, location="us-central1")

bq_client = bigquery.Client()
embedding_model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")

# ==============================
# Helper: batches de 50
# ==============================
def batch_rows(rows, batch_size=50):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]

# =========================================================
# Inserción normal
# =========================================================
def insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    rows = []
    intents_repeated = {
        "aportacionesobreropatronales",
        "aportacionespatronales",
        "aportacionesentucuentaindividual",
        "continuacionmodalidadcuarenta",
        "requisitosmodalidadcuarenta",
        "beneficiosdelamodalidadcuarenta",
        "pagomodalidadcuarenta",
        "inscripcionmodalidadcuarenta",
        "consideracionesdelamodalidadcuarenta",
    }

    for i, parrafo in enumerate(parrafos_con_intenciones):
        embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values

        rows.append({
            "id": f"{topic}_{parrafo['intent']}_chunk_{i}",
            "channel": channel.lower().strip(),
            "name_document": documento.strip(),
            "chunk_id": i,
            "text": parrafo["texto"].strip(),
            "topic": topic.lower().strip(),
            "intent": parrafo["intent"].lower().strip(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": "S" if parrafo["intent"].lower() in intents_repeated else "N",
        })

    # Insertar en BigQuery en batches de 50
    for idx, rows_batch in enumerate(batch_rows(rows, 50), start=1):
        errors = bq_client.insert_rows_json(table_ref, rows_batch)
        if errors:
            raise Exception(f"Error insertando en BigQuery (batch {idx}): {errors}")

# =========================================================
# Inserción BETA (con borrado previo)
# =========================================================
def insertar_chunks_en_bigquery_beta(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery beta, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_BETA}"

    # -----------------------------
    # 1️⃣ Eliminar registros existentes
    # -----------------------------
    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE LOWER(knowledge_domain) = @topic
      AND LOWER(channel) = @channel
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("topic", "STRING", topic.lower().strip()),
            bigquery.ScalarQueryParameter("channel", "STRING", channel.lower().strip()),
        ]
    )

    bq_client.query(delete_query, job_config=job_config).result()

    # -----------------------------
    # 2️⃣ Construir filas
    # -----------------------------
    rows = []
    intents_repeated = {
        "aportacionesobreropatronales",
        "aportacionespatronales",
        "aportacionesentucuentaindividual",
        "continuacionmodalidadcuarenta",
        "requisitosmodalidadcuarenta",
        "beneficiosdelamodalidadcuarenta",
        "pagomodalidadcuarenta",
        "inscripcionmodalidadcuarenta",
        "consideracionesdelamodalidadcuarenta",
    }

    for i, parrafo in enumerate(parrafos_con_intenciones):
        embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values

        rows.append({
            "id": f"{topic}_{parrafo['intent']}_chunk_{i}",
            "channel": channel.lower().strip(),
            "name_document": documento.strip(),
            "chunk_id": i,
            "text": parrafo["texto"].strip(),
            "knowledge_domain": topic.lower().strip(),
            "intent": parrafo["intent"].lower().strip(),
            "intent_document": parrafo["intent"].strip(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": "S" if parrafo["intent"].lower() in intents_repeated else "N",
        })

    # -----------------------------
    # 3️⃣ Insertar en batches de 50
    # -----------------------------
    for idx, rows_batch in enumerate(batch_rows(rows, 50), start=1):
        errors = bq_client.insert_rows_json(table_ref, rows_batch)
        if errors:
            raise Exception(f"Error insertando en BigQuery BETA (batch {idx}): {errors}")
