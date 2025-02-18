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

    subintencion_actual = "Sin categoría"
    parrafos_con_intenciones = []

    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    texto = span["text"].strip()

                    # Si el texto está en negritas y mayúsculas, lo consideramos una subintención
                    if texto and span.get("bold", False) and texto.isupper():
                        subintencion_actual = texto  # Asignamos como subintención
                    else:
                        # Guardamos el párrafo con la intención y la subintención actual
                        parrafos_con_intenciones.append(
                            {
                                "intencion": intencion,
                                "subintencion": subintencion_actual,
                                "texto": texto,
                            }
                        )

    return parrafos_con_intenciones
