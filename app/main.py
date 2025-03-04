from fastapi import FastAPI
from pydantic import BaseModel
from app.extractor import extraer_texto_con_intenciones
from app.bigquery import insertar_chunks_en_bigquery
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel
import math
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
    channel: str


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
        question_embedding = embedding_model.get_embeddings([request.question])[
            0
        ].values

        # Construir la consulta filtrando solo por topic
        query = f"""
            SELECT id, name_document, text, embedding
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE topic = '{request.topic}' AND channel = '{request.channel}'
        """
        query_job = bq_client.query(query)
        rows = query_job.result()
        query_job = bq_client.query(query)
        rows = query_job.result()

        mejores_respuestas = []
        best_match = None
        best_similarity = -1  # Inicializamos con valor muy bajo

        # Función para calcular la similitud coseno
        def cosine_similarity(vec_a, vec_b):
            dot = sum(a * b for a, b in zip(vec_a, vec_b))
            norm_a = math.sqrt(sum(a * a for a in vec_a))
            norm_b = math.sqrt(sum(b * b for b in vec_b))
            return dot / (norm_a * norm_b) if norm_a and norm_b else 0

        for row in rows:
            chunk_embedding = row["embedding"]
            similarity = cosine_similarity(question_embedding, chunk_embedding)
            print("### Similaridad ###")
            print(similarity)
            # Actualizar mejor respuesta si se encuentra una similitud mayor
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = row
            # Si la similitud supera el umbral 0.5, se agrega al array de respuestas
            if similarity > 0.7:
                mejores_respuestas.append(
                    {
                        "respuesta": row["text"],
                        "documento": row["name_document"],
                        "similarity": similarity,
                    }
                )
            print("### Fin Similaridad ###")

        # Si se encontró alguna respuesta
        if best_match:
            return {
                "mejor_respuesta": {
                    "respuesta": best_match["text"],
                    "documento": best_match["name_document"],
                    "similarity": best_similarity,
                },
                "otras_respuestas": mejores_respuestas,
            }
        else:
            return {
                "mensaje": "No se encontró ningún chunk que coincida con la búsqueda."
            }
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
            return {
                "mensaje": "No se encontró ningún chunk que coincida con la búsqueda."
            }
