from airflow.decorators import dag, task
from datetime import datetime
import logging

# Importamos los scripts principales del ETL
from extract import main as extract_main
from transformation import transform_data as transformation_main
from load import main as load_main

# Configurar logging de Airflow
logger = logging.getLogger("airflow.task")

# -----------------------------
# Definir el DAG
# -----------------------------
@dag(
    dag_id="etl_workflow",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1

)
def etl_pipeline():

    @task()
    def extract():
        try:
            logger.info("Starting data extraction...")
            extract_main()
            logger.info("Extraction completed successfully.")
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

    @task()
    def transform():
        try:
            logger.info("Starting data transformation...")
            transformation_main()
            logger.info("Transformation completed successfully.")
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise  

    @task()
    def load():
        try:
            logger.info("Starting data load...")
            load_main()
            logger.info("Load completed successfully.")
        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

    # -----------------------------
    # Configurar dependencias
    # -----------------------------
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    extract_task >> transform_task >> load_task

# Instanciar el DAG
etl_pipeline()