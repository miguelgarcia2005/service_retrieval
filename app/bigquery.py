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


def insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    rows = []
    intents_repeated = [
        "ModalidadCuarenta",
        "RequisitosModalidadCuarenta",
        "BeneficiosDeLaModalidadCuarenta",
        "PagoModalidadCuarenta",
        "InscripcionModalidadCuarenta",
        "ConsideracioneDeLaModalidadCuarenta",
    ]

    for i, parrafo in enumerate(parrafos_con_intenciones):
        # Generar el embedding para el párrafo (chunk)
        embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values
        is_repeat = "S" if parrafo["intencion"] in intents_repeated else "N"
        # Construir el registro con el embedding incluido
        row = {
            "id": f"{topic}_{parrafo['intencion']}_chunk_{i}",
            "channel": channel.lower(),
            "name_document": documento,  # Puedes modificar esto si hay un campo de documento
            "chunk_id": i,
            "text": parrafo["texto"],
            "topic": topic.lower(),
            "intent": parrafo["intencion"].lower(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": is_repeat,
        }
        # print(f"##### El valor del embedding es: {embedding} ####\n\n")
        rows.append(row)

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")


def insertar_chunks_en_bigquery_beta(
    parrafos_con_intenciones, documento, topic, channel
):
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
        is_repeat = "S" if parrafo["intencion"] in intents_repeated else "N"
        # Construir el registro con el embedding incluido
        row = {
            "id": f"{topic}_{parrafo['intencion']}_chunk_{i}",
            "channel": channel.lower().strip(),
            "name_document": documento.strip(),
            "chunk_id": i,
            "text": parrafo["texto"].strip(),
            "knowledge_domain": topic.lower().strip(),
            "intent": parrafo["intencion"].lower().strip(),
            "intent_document": parrafo["intencion"].strip(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": is_repeat,
        }
        # print(f"##### El valor del embedding es: {embedding} ####\n\n")
        rows.append(row)

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Error insertando en BigQuery: {errors}")
