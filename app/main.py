from fastapi import FastAPI
from app.extractor import extraer_texto_por_parrafos
from app.bigquery import insertar_chunks_en_bigquery

app = FastAPI()

@app.post("/procesar-documento/")
def procesar_documento(documento: str, intencion: str, subintencion: str):
    """Extrae texto de un documento y lo almacena en BigQuery"""
    parrafos = extraer_texto_por_parrafos(documento)
    insertar_chunks_en_bigquery(documento, parrafos, intencion, subintencion)
    return {"mensaje": f"{len(parrafos)} chunks procesados y almacenados en BigQuery"}
