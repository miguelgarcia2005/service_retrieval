import fitz  # pymupdf para extraer texto de PDFs
from google.cloud import storage
import os
from dotenv import load_dotenv
import re

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
storage_client = storage.Client()

def extraer_texto_con_intenciones(blob_name):
    print("Entrando en extracción de documentos")

    """Descarga un PDF desde Cloud Storage, detecta títulos como intenciones y extrae párrafos"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{blob_name}")
    pdf_data = blob.download_as_bytes()

    doc = fitz.open(stream=pdf_data, filetype="pdf")
    intencion_actual = None
    parrafos_con_intenciones = []

    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        print(f"Estos son los blocks: {blocks}")
        for block in blocks:
            for line in block.get("lines", []):
                # Reconstruir la línea uniendo todos los spans
                line_text = " ".join(span["text"].strip() for span in line.get("spans", [])).strip()
                if not line_text:
                    continue

                # Verificar si la línea es un título (negrita y mayúsculas)
                es_titulo = any(
                    span.get("bold", False) and  # Negrita
                    re.match(r"^[A-Z_]+$", span["text"].strip())  # Mayúsculas y guiones bajos
                    for span in line.get("spans", [])
                )

                if es_titulo:
                    # Actualizamos la intención con el título detectado
                    intencion_actual = line_text
                else:
                    # Si ya se ha detectado una intención, guardamos la línea como párrafo asociado
                    if intencion_actual:
                        # Si el párrafo ya existe, añadimos la línea al texto existente
                        if parrafos_con_intenciones and parrafos_con_intenciones[-1]["intencion"] == intencion_actual:
                            parrafos_con_intenciones[-1]["texto"] += " " + line_text
                        else:
                            # Si no, creamos un nuevo párrafo
                            parrafos_con_intenciones.append({
                                "intencion": intencion_actual,
                                "texto": line_text
                            })

    print(parrafos_con_intenciones)
    return parrafos_con_intenciones