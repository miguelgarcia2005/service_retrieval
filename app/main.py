from fastapi import FastAPI
from pydantic import BaseModel
from app.extractor import extraer_texto_por_parrafos
from app.bigquery import insertar_chunks_en_bigquery
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel

app = FastAPI()

@app.post("/procesar-documento/")
def procesar_documento(documento: str, intencion: str, subintencion: str):
    """Extrae texto de un documento y lo almacena en BigQuery"""
    parrafos = extraer_texto_por_parrafos(documento)
    insertar_chunks_en_bigquery(documento, parrafos, intencion, subintencion)
    return {"mensaje": f"{len(parrafos)} chunks procesados y almacenados en BigQuery"}
    
# Modelo de datos para la búsqueda
class SearchRequest(BaseModel):
    question: str
    intencion: str
    subintencion: str

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
    1. Convierte la pregunta a un embedding.
    2. Filtra los chunks en BigQuery por 'intent' y 'subintencion'.
    3. Calcula la distancia euclidiana entre el embedding de la pregunta y cada embedding del chunk.
    4. Retorna el chunk con la menor distancia (el más relevante).
    """
    # Convertir la pregunta a embedding
    question_embedding = embedding_model.get_embeddings([request.question])[0].values

    # Construir la consulta filtrando por intención y subintención
    query = f"""
        SELECT id, name_document, text, embedding
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE intent = '{request.intencion}' AND sub_intent = '{request.subintencion}'
    """
    query_job = bq_client.query(query)
    rows = query_job.result()

    # Buscar el chunk más relevante calculando la distancia euclidiana
    best_match = None
    min_distance = float("inf")
    for row in rows:
        chunk_embedding = row["embedding_value"]
        distance = sum((a - b) ** 2 for a, b in zip(chunk_embedding, question_embedding)) ** 0.5
        if distance < min_distance:
            min_distance = distance
            best_match = row

    # Retornar la respuesta si se encontró un match
    if best_match:
        return {
            "respuesta": best_match["text"],
            "documento": best_match["name_document"],
            "distancia": min_distance
        }
    else:
        return {"mensaje": "No se encontró ningún chunk que coincida con la búsqueda."}