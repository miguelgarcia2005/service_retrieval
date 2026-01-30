from google.cloud import bigquery
import os
import logging
import uuid
from datetime import datetime
from dotenv import load_dotenv
from vertexai.language_models import TextEmbeddingModel
import vertexai

# ==============================
# Configurar logging
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================
# Cargar variables de entorno
# ==============================
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TABLE_ID_BETA = os.getenv("TABLE_ID_BETA")

# ==============================
# Inicializar Vertex AI y BigQuery
# ==============================
vertexai.init(project=PROJECT_ID, location="us-central1")

bq_client = bigquery.Client()
embedding_model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")

# ==============================
# Helper: batches de 50
# ==============================
def batch_rows(rows, batch_size=50):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]

# =========================================================
# Inserción normal
# =========================================================
def insertar_chunks_en_bigquery(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery, generando el embedding para cada uno."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Generar un identificador único para esta ejecución
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S") + "_" + str(uuid.uuid4())[:8]
    
    logger.info(f"=== INICIO INSERCIÓN ===")
    logger.info(f"Execution ID: {execution_id}")
    logger.info(f"Total de párrafos a insertar: {len(parrafos_con_intenciones)}")
    logger.info(f"Topic: {topic}, Channel: {channel}, Documento: {documento}")

    rows = []
    intents_repeated = {
        "aportacionesobreropatronales",
        "aportacionespatronales",
        "aportacionesentucuentaindividual",
        "continuacionmodalidadcuarenta",
        "requisitosmodalidadcuarenta",
        "beneficiosdelamodalidadcuarenta",
        "pagomodalidadcuarenta",
        "inscripcionmodalidadcuarenta",
        "consideracionesdelamodalidadcuarenta",
    }

    for i, parrafo in enumerate(parrafos_con_intenciones):
        try:
            embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values
        except Exception as e:
            logger.error(f"Error generando embedding para párrafo {i}: {e}")
            raise Exception(f"Error generando embedding para párrafo {i}: {e}")

        # ID único usando execution_id para evitar colisiones
        unique_id = f"{topic}_{parrafo['intent']}_chunk_{i}_{execution_id}"
        
        rows.append({
            "id": unique_id,
            "channel": channel.lower().strip(),
            "name_document": documento.strip(),
            "chunk_id": i,
            "text": parrafo["texto"].strip(),
            "topic": topic.lower().strip(),
            "intent": parrafo["intent"].lower().strip(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": "S" if parrafo["intent"].lower() in intents_repeated else "N",
        })

    logger.info(f"Total de filas preparadas: {len(rows)}")

    # Insertar en BigQuery en batches de 50
    total_insertados = 0
    total_batches = (len(rows) + 49) // 50  # Calcular número total de batches
    
    for idx, rows_batch in enumerate(batch_rows(rows, 50), start=1):
        logger.info(f"Insertando batch {idx}/{total_batches} con {len(rows_batch)} registros...")
        errors = bq_client.insert_rows_json(table_ref, rows_batch)
        
        if errors:
            logger.error(f"Error en batch {idx}: {errors}")
            # Log detallado de cada error
            for error in errors:
                logger.error(f"  - Error detalle: {error}")
            raise Exception(f"Error insertando en BigQuery (batch {idx}): {errors}")
        else:
            total_insertados += len(rows_batch)
            logger.info(f"Batch {idx} insertado exitosamente. Total acumulado: {total_insertados}/{len(rows)}")
    
    logger.info(f"=== FIN INSERCIÓN ===")
    logger.info(f"Total registros insertados: {total_insertados}")
    
    return total_insertados

# =========================================================
# Inserción BETA (con borrado previo) - Usando LOAD en lugar de streaming
# =========================================================
def insertar_chunks_en_bigquery_beta(parrafos_con_intenciones, documento, topic, channel):
    """Inserta los chunks extraídos en BigQuery beta usando load job (más confiable que streaming)."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_BETA}"
    
    logger.info(f"=== INICIO INSERCIÓN BETA ===")
    logger.info(f"Total de párrafos a insertar: {len(parrafos_con_intenciones)}")
    logger.info(f"Topic: {topic}, Channel: {channel}, Documento: {documento}")

    # -----------------------------
    # 1️⃣ Eliminar registros existentes
    # -----------------------------
    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE LOWER(knowledge_domain) = @topic
      AND LOWER(channel) = @channel
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("topic", "STRING", topic.lower().strip()),
            bigquery.ScalarQueryParameter("channel", "STRING", channel.lower().strip()),
        ]
    )

    logger.info(f"Ejecutando DELETE para topic={topic.lower().strip()}, channel={channel.lower().strip()}")
    delete_job = bq_client.query(delete_query, job_config=job_config)
    delete_job.result()  # Esperar a que termine
    logger.info(f"DELETE completado. Registros eliminados: {delete_job.num_dml_affected_rows}")

    # -----------------------------
    # 2️⃣ Construir filas
    # -----------------------------
    rows = []
    intents_repeated = {
        "aportacionesobreropatronales",
        "aportacionespatronales",
        "aportacionesentucuentaindividual",
        "continuacionmodalidadcuarenta",
        "requisitosmodalidadcuarenta",
        "beneficiosdelamodalidadcuarenta",
        "pagomodalidadcuarenta",
        "inscripcionmodalidadcuarenta",
        "consideracionesdelamodalidadcuarenta",
    }

    for i, parrafo in enumerate(parrafos_con_intenciones):
        try:
            embedding = embedding_model.get_embeddings([parrafo["texto"]])[0].values
        except Exception as e:
            logger.error(f"Error generando embedding para párrafo {i}: {e}")
            raise Exception(f"Error generando embedding para párrafo {i}: {e}")

        rows.append({
            "id": f"{topic}_{parrafo['intent']}_chunk_{i}",
            "channel": channel.lower().strip(),
            "name_document": documento.strip(),
            "chunk_id": i,
            "text": parrafo["texto"].strip(),
            "knowledge_domain": topic.lower().strip(),
            "intent": parrafo["intent"].lower().strip(),
            "intent_document": parrafo["intent"].strip(),
            "is_transactional": "N",
            "embedding": embedding,
            "is_repeat": "S" if parrafo["intent"].lower() in intents_repeated else "N",
        })

    logger.info(f"Total de filas preparadas: {len(rows)}")

    # -----------------------------
    # 3️⃣ Insertar usando LOAD JOB (más confiable que streaming)
    # -----------------------------
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    logger.info(f"Iniciando load job para {len(rows)} registros...")
    
    load_job = bq_client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config
    )
    
    # Esperar a que el job termine
    load_job.result()
    
    # Verificar resultado
    if load_job.errors:
        logger.error(f"Errores en load job: {load_job.errors}")
        raise Exception(f"Error en load job: {load_job.errors}")
    
    total_insertados = load_job.output_rows
    logger.info(f"=== FIN INSERCIÓN BETA ===")
    logger.info(f"Load job completado. Registros insertados: {total_insertados}")
    
    return total_insertados
