import fitz  # PyMuPDF
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
        # Extraer el texto de la página en formato "dict"
        page_dict = page.get_text("dict")
        blocks = page_dict.get("blocks", [])

        for block in blocks:
            if "lines" not in block:
                continue

            for line in block["lines"]:
                line_text = " ".join(span["text"].strip() for span in line.get("spans", [])).strip()
                if not line_text:
                    continue

                # Verificar si la línea es un título (negrita y fuente Calibri-Bold)
                # es_titulo = any(
                #     span.get("flags", 0) == 16 and  # Negrita
                #     span.get("font", "").startswith("Calibri-Bold") and  # Fuente Calibri-Bold
                #     re.match(r"^[A-Z_ ]+$", span["text"].strip())  # Mayúsculas, guiones bajos y espacios
                #     for span in line.get("spans", [])
                # )
                # es_titulo = any(
                #     span.get("flags", 0) == 16 and  # Negrita
                #     span.get("font", "").startswith("Calibri-Bold") and 
                #     (re.match(r"^[A-Z][a-z]+(?:[A-Z][a-z]+)*$", span["text"].strip()) or re.match(r"^[A-Za-z]+$", span["text"].strip()))  # Palabra simple
                #     for span in line.get("spans", [])
                # )
                match = re.match(r"^([A-Z][a-z]+(?:[A-Z][a-z]+)*)(?:_([a-z]+))?$", text)

                # if es_titulo:
                #     print(line_text)
                #     # Actualizamos la intención con el título detectado
                #     intencion_actual = line_text
                if match:
                    intencion_actual = match.group(1)
                    subtitulo = match.group(2) if match.group(2) else None
                    print(f"Título: {intencion_actual}, Subtítulo: {subtitulo}")
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

    # print(parrafos_con_intenciones)
    return parrafos_con_intenciones