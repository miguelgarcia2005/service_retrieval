from fastapi import FastAPI
from pydantic import BaseModel
from app.extractor import extraer_texto_con_intenciones
from app.bigquery import insertar_chunks_en_bigquery
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel
import numpy as np
from google.api_core.exceptions import GoogleAPICallError
import time

app = FastAPI()


@app.post("/procesar-documento/")
def procesar_documento(documento: str, topic: str):
    """Extrae el texto del documento, asigna subintenciones y lo almacena en BigQuery"""
    parrafos_con_intenciones = extraer_texto_con_intenciones(documento)
    insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic)
    print("###### PARRAFOS CON INTENCIONS COMPLETAS ####")
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
    try:
        # Verificar si el intent es nulo o vacío
        if not request.intent:
            # Convertir la pregunta a embedding
            question_embedding = embedding_model.get_embeddings([request.question])[
                0
            ].values
            start_time_query = time.time() 
            # Construir la consulta filtrando solo por topic
            query = f"""
                SELECT id, name_document, text, embedding
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                WHERE topic = '{request.topic}' AND channel = '{request.channel}'
            """
            query_job = bq_client.query(query)
            rows = query_job.result()
            end_time_query = time.time()  # Fin de la medición
            time_execution_query = end_time_query - start_time_query
            # Lista para almacenar similitudes y respuestas
            similarities = []

            # Función para calcular la similitud coseno
            def cosine_similarity(vec_a, vec_b):
                dot = np.dot(vec_a, vec_b)
                norm_a = np.linalg.norm(vec_a)
                norm_b = np.linalg.norm(vec_b)
                return dot / (norm_a * norm_b) if norm_a and norm_b else 0

            start_time = time.time()  # Inicio de la medición
            # Calcular similitudes para cada fila
            for row in rows:
                chunk_embedding = row["embedding"]
                similarity = cosine_similarity(question_embedding, chunk_embedding)
                similarities.append(
                    {
                        "text": row[
                            "text"
                        ],  # Solo almacenamos el texto de la respuesta
                        "similarity": similarity,
                    }
                )
            end_time = time.time()  # Fin de la medición
            time_execution = end_time - start_time
            # print("Tiempo de ejecución:", end_time - start_time, "segundos")
            # Ordenar por similitud (de mayor a menor)
            similarities.sort(key=lambda x: x["similarity"], reverse=True)

            # Seleccionar el top 3 de respuestas (solo el texto)
            top_responses = [resp["text"] for resp in similarities[:3]]

            # Retornar el top 3 de respuestas
            return {
                "response": top_responses,
                "knowledge_domain": request.topic,
                "transactional_or_non_transactional": "non_transactional",
                "time_execution": time_execution,
                "query_time_execution" : time_execution_query
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
                    "response": [row["text"]],  # Solo el texto de la respuesta
                }
            else:
                return {
                    "response": [],  # No se encontró ninguna respuesta
                }
    except GoogleAPICallError as e:
        return {"error": f"Error al consultar BigQuery: {str(e)}"}
    except Exception as e:
        return {"error": f"Error inesperado: {str(e)}"}
