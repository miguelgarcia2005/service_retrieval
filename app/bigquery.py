from google.cloud import bigquery
import os
from dotenv import load_dotenv
from vertexai.language_models import TextEmbeddingModel
import vertexai
# Cargar variables de entorno
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TABLE_ID_BETA = os.getenv("TABLE_ID_BETA")

# Inicializar el cliente de BigQuery y el modelo de embeddings
# bq_client = bigquery.Client()
# embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")


# Inicializar Vertex AI y modelo
vertexai.init(project=PROJECT_ID, location="us-central1")
bq_client = bigquery.Client()
embedding_model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")

def insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    rows = []
    intents_repeated = [
        "AportacionesObreroPatronales",
        "AportacionesPatronales",
        "AportacionesEnTuCuentaIndividual",
        "ContinuacionModalidadCuarenta",
        "RequisitosModalidadCuarenta",
        "BeneficiosDeLaModalidadCuarenta",
        "PagoModalidadCuarenta",
        "InscripcionModalidadCuarenta",
        "ConsideracionesDeLaModalidadCuarenta",
    ]

    for i, parrafo in enumerate(parrafos_con_intenciones):
        # Generar el embedding para el párrafo (chunk)
        embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values
        is_repeat = "S" if parrafo["intent"] in intents_repeated else "N"
        # Construir el registro con el embedding incluido
        row = {
            "id": f"{topic}_{parrafo['intent']}_chunk_{i}",
            "channel": channel.lower(),
            "name_document": documento,  # Puedes modificar esto si hay un campo de documento
            "chunk_id": i,
            "text": parrafo["texto"],
            "topic": topic.lower(),
            "intent": parrafo["intent"].lower(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": is_repeat,
        }
        # print(f"##### El valor del embedding es: {embedding} ####\n\n")
        rows.append(row)

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")



def insertar_chunks_en_bigquery_beta(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery beta, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_BETA}"
    rows = []
    intents_repeated = [
        "AportacionesObreroPatronales",
        "AportacionesPatronales",   
        "AportacionesEnTuCuentaIndividual",
        "ContinuacionModalidadCuarenta",
        "RequisitosModalidadCuarenta",
        "BeneficiosDeLaModalidadCuarenta",
        "PagoModalidadCuarenta",
        "InscripcionModalidadCuarenta",
        "ConsideracionesDeLaModalidadCuarenta",
    ]

    for i, parrafo in enumerate(parrafos_con_intenciones):
        embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values
        is_repeat = "S" if parrafo["intent"] in intents_repeated else "N"
        row = {
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
            "is_repeat": is_repeat,
        }
        rows.append(row)

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")