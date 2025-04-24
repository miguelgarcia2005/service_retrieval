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
                line_text = " ".join(
                    span["text"].strip() for span in line.get("spans", [])
                ).strip()
                if not line_text:
                    continue

                # Verificar si la línea es un título (negrita y fuente Calibri-Bold)
                # es_titulo = any(
                #     span.get("flags", 0) == 16 and  # Negrita
                #     span.get("font", "").startswith("Calibri-Bold") and  # Fuente Calibri-Bold
                #     re.match(r"^[A-Z_ ]+$", span["text"].strip())  # Mayúsculas, guiones bajos y espacios
                #     for span in line.get("spans", [])
                # )
                es_titulo = any(
                    span.get("flags", 0) == 16  # Negrita
                    and span.get("font", "").startswith("Calibri-Bold")
                    and (
                        re.match(r"^[A-Z][a-z]+(?:[A-Z][a-z]+)*$", span["text"].strip())
                        or re.match(r"^[A-Za-z]+$", span["text"].strip())
                    )  # Palabra simple
                    for span in line.get("spans", [])
                )

                if es_titulo:
                    print(
                        f"Este es el titulo: {line_text} y esta es la intención actual {intencion_actual}"
                    )
                    if intencion_actual and intencion_actual == line_text:
                        parrafos_con_intenciones.append(
                            {"intencion": intencion_actual, "texto": line_text}
                        )
                    # Actualizamos la intención con el título detectado
                    intencion_actual = line_text
                else:
                    # Si ya se ha detectado una intención, guardamos la línea como párrafo asociado
                    if intencion_actual:
                        # Si el párrafo ya existe, añadimos la línea al texto existente
                        if (
                            parrafos_con_intenciones
                            and parrafos_con_intenciones[-1]["intencion"]
                            == intencion_actual
                        ):
                            # print(f"Esta texto es la prueba: {intencion_actual} y el texto asociado es {line_text}")
                            parrafos_con_intenciones[-1]["texto"] += " " + line_text
                        else:
                            # Si no, creamos un nuevo párrafo
                            parrafos_con_intenciones.append(
                                {"intencion": intencion_actual, "texto": line_text}
                            )

    # print(parrafos_con_intenciones)
    return parrafos_con_intenciones


def normalizar_intencion(texto: str) -> dict:
    """
    Procesa títulos en formato 'CamelCase_subtitulo' y devuelve un diccionario con:
    - titulo_principal: Parte CamelCase (se mantiene igual)
    - subtitulo: Parte después del guión bajo (en minúsculas)
    - intent_completa: Combinación original (se mantiene igual)
    """
    if "_" not in texto:
        raise ValueError(
            f"El título '{texto}' no contiene guión bajo para separar subtítulo"
        )

    titulo_principal, subtitulo = texto.split("_", 1)

    # Validar que el título principal sea CamelCase válido
    if not re.match(r"^[A-Z][a-z]+(?:[A-Z][a-z]+)*$", titulo_principal):
        raise ValueError(f"Formato CamelCase inválido en título: '{titulo_principal}'")

    return {
        "intent": titulo_principal,
        "subtitle": subtitulo.lower(),
        "intent_document": titulo_principal,
    }


def es_titulo_valido(line_text: str, spans: list) -> bool:
    """
    Verifica si el texto cumple con:
    1. Formato: 'CamelCase_subtitulo'
    2. Estilo: Negrita (flags == 16) y fuente Calibri-Bold
    """
    # Verificar formato CamelCase_subtitulo
    formato_valido = (
        re.match(r"^[A-Z][a-z]+(?:[A-Z][a-z]+)*_[a-z0-9_]+$", line_text.strip())
        is not None
    )

    # Verificar estilo de fuente
    estilo_valido = any(
        span.get("flags", 0) == 16  # Negrita
        and "Calibri-Bold" in span.get("font", "")  # Fuente Calibri-Bold
        for span in spans
    )

    return formato_valido and estilo_valido


def extraer_texto_con_intenciones_beta(blob_name: str):
    """Descarga un PDF y extrae texto organizado por títulos y subtítulos"""
    print(f"Procesando archivo: {blob_name}")

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{blob_name}")
        pdf_data = blob.download_as_bytes()

        doc = fitz.open(stream=pdf_data, filetype="pdf")
        datos_extraidos = []
        contexto_actual = None

        for page in doc:
            page_dict = page.get_text("dict")
            blocks = page_dict.get("blocks", [])

            for block in blocks:
                if "lines" not in block:
                    continue

                for line in block["lines"]:
                    line_text = " ".join(
                        span["text"].strip() for span in line.get("spans", [])
                    ).strip()
                    if not line_text:
                        continue

                    if es_titulo_valido(line_text, line.get("spans", [])):
                        contexto_actual = normalizar_intencion(line_text)
                        print(
                            f"Contexto detectado: {contexto_actual['intent_document']}"
                        )
                    elif contexto_actual:
                        # Buscar si ya existe un bloque con este contexto
                        bloque_existente = next(
                            (
                                b
                                for b in datos_extraidos
                                if b["intent_document"]
                                == contexto_actual["intent_document"]
                            ),
                            None,
                        )

                        if bloque_existente:
                            bloque_existente["texto"] += " " + line_text
                        else:
                            nuevo_bloque = {**contexto_actual, "texto": line_text}
                            datos_extraidos.append(nuevo_bloque)

        print(f"Extracción completada. Bloques encontrados: {len(datos_extraidos)}")
        return datos_extraidos

    except Exception as e:
        print(f"Error procesando {blob_name}: {str(e)}")
        raise
