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
    """Descarga un PDF desde Cloud Storage y extrae texto dividido en párrafos"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"Chat/Saldos/Informativo/{blob_name}")
    pdf_data = blob.download_as_bytes()

    doc = fitz.open(stream=pdf_data, filetype="pdf")
    texto_completo = "\n".join([page.get_text("text") for page in doc])

    # Dividir el texto en párrafos
    parrafos = nltk.tokenize.sent_tokenize(texto_completo)

    return parrafos
