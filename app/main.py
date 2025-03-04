from fastapi import FastAPI
from pydantic import BaseModel
from app.extractor import extraer_texto_con_intenciones
from app.bigquery import insertar_chunks_en_bigquery
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel

app = FastAPI()


@app.post("/procesar-documento/")
def procesar_documento(documento: str, topic: str):
    """Extrae el texto del documento, asigna subintenciones y lo almacena en BigQuery"""
    parrafos_con_intenciones = extraer_texto_con_intenciones(documento)
    insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic)
    print("###### PARRAFOS CON INTENCIONS ####")
    print(parrafos_con_intenciones)
    return {
        "mensaje": f"{len(parrafos_con_intenciones)} chunks procesados y almacenados en BigQuery"
    }


# Modelo de datos para la búsqueda
class SearchRequest(BaseModel):
    question: str
    intent: str
    topic: str
    channel : str


# Configuración para la búsqueda: BigQuery y modelo de embeddings
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

bq_client = bigquery.Client()
embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")


# Endpoint para buscar la respuesta a partir de una pregunta, intención y subintención
@app.post("/buscar/")
def buscar(request: SearchRequest):
    """
    1. Si el intent es nulo o vacío, busca todas las filas con el topic y realiza la comparación de embeddings.
    2. Si el intent no es nulo ni vacío, busca la fila que coincida con el intent, topic y channel.
    """
    # Verificar si el intent es nulo o vacío
    if not request.intent:
        # Convertir la pregunta a embedding
        question_embedding = embedding_model.get_embeddings([request.question])[0].values

        # Construir la consulta filtrando solo por topic
        query = f"""
            SELECT id, name_document, text, embedding
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE topic = '{request.topic}' AND channel = '{request.channel}'
        """
        query_job = bq_client.query(query)
        rows = query_job.result()

        # Buscar el chunk más relevante calculando la distancia euclidiana
        best_match = None
        min_distance = float("inf")
        for row in rows:
            chunk_embedding = row["embedding"]
            distance = (
                sum((a - b) ** 2 for a, b in zip(chunk_embedding, question_embedding))
                ** 0.5
            )
            if distance < min_distance:
                min_distance = distance
                best_match = row

        # Retornar la respuesta si se encontró un match
        if best_match:
            return {
                "respuesta": best_match["text"],
                "documento": best_match["name_document"],
                "distancia": min_distance,
            }
        else:
            return {"mensaje": "No se encontró ningún chunk que coincida con la búsqueda."}
    else:
        # Construir la consulta filtrando por intent, topic y channel
        query = f"""
            SELECT id, name_document, text
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE intent = '{request.intent}'
              AND topic = '{request.topic}'
              AND channel = '{request.channel}'
        """
        query_job = bq_client.query(query)
        rows = query_job.result()

        # Obtener la primera fila (debería ser única)
        row = next(rows, None)

        # Retornar la respuesta si se encontró un match
        if row:
            return {
                "respuesta": row["text"],
                "documento": row["name_document"],
                "distancia": 0,  # No hay distancia porque no se comparó con embeddings
            }
        else:
            return {"mensaje": "No se encontró ningún chunk que coincida con la búsqueda."}