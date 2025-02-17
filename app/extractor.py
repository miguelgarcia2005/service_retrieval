import fitz  # pymupdf para extraer texto de PDFs
import nltk
from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
storage_client = storage.Client()

nltk.download("punkt")


def extraer_texto_por_parrafos(blob_name):
    print("Entrando en extracción de documentos")
    """Descarga un PDF desde Cloud Storage y extrae texto dividido en párrafos"""
    bucket = storage_client.bucket(BUCKET_NAME)
    print(f"El bucket es {bucket}")
    blob = bucket.blob(f"{blob_name}")
    print(f"La ruta del bucket es {blob}")
    pdf_data = blob.download_as_bytes()

    doc = fitz.open(stream=pdf_data, filetype="pdf")
    texto_completo = "\n".join([page.get_text("text") for page in doc])

    # Dividir el texto en párrafos
    parrafos = [p.strip() for p in texto_completo.split("\n\n") if p.strip()]

    return parrafos
