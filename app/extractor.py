import fitz  # pymupdf para extraer texto de PDFs
import nltk
import re
from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
storage_client = storage.Client()

nltk.download("punkt")


def extraer_texto_con_intenciones(blob_name, intencion):
    print("Entrando en extracción de documentos")

    """Descarga un PDF desde Cloud Storage, detecta títulos como subintenciones y extrae párrafos"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{blob_name}")
    pdf_data = blob.download_as_bytes()

    doc = fitz.open(stream=pdf_data, filetype="pdf")
    intencion_actual = None
    parrafos_con_intenciones = []

    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            for line in block.get("lines", []):
                # Reconstruir la línea uniendo todos los spans
                line_text = " ".join(span["text"].strip() for span in line.get("spans", [])).strip()
                if not line_text:
                    continue

                # Verificar si la línea tiene al menos un span en negrita
                es_titulo = any(span.get("bold", False) and span["text"].strip() for span in line.get("spans", []))
                if es_titulo:
                    # Actualizamos la intención con el título detectado
                    intencion_actual = line_text
                else:
                    # Si ya se ha detectado una intención, guardamos la línea como párrafo asociado
                    if intencion_actual:
                        parrafos_con_intenciones.append({
                            "intencion": intencion_actual,
                            "texto": line_text
                        })
    print(parrafos_con_intenciones)
    return parrafos_con_intenciones
