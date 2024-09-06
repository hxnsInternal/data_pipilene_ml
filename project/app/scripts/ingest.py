import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def ingest_data(spark: SparkSession, config: dict):

    """
    Ingresa datos desde las fuentes especificadas en la configuración, como archivos JSON y CSV.

    Args:
        spark (SparkSession): La sesión activa de PySpark.
        config (dict): Diccionario con la configuración de ingesta de datos. Debe incluir:
            - ingest['files']: Lista de archivos a procesar.
            - Cada archivo debe tener la clave 'path' con la ruta al archivo y la opción 'options' para archivos CSV.
    
    Returns:
        tuple: Una tupla que contiene los DataFrames:
            - prints_df (DataFrame): Datos leídos desde el archivo JSON de prints.
            - taps_df (DataFrame): Datos leídos desde el archivo JSON de taps.
            - payments_df (DataFrame): Datos leídos desde el archivo CSV de pagos.
    
    Raises:
        FileNotFoundError: Si alguno de los archivos especificados en las rutas no se encuentra.
        AnalysisException: Si ocurre un error relacionado con Spark durante la lectura de los archivos.
        Exception: Cualquier otro error inesperado que ocurra durante la ingesta de datos.
    """

    try:
        logging.info("Ingesting data from sources...")
        
        # Obtener la ruta base del proyecto dinámicamente
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Obtener la ruta de los archivos input

        prints_path = os.path.join(BASE_DIR, config['ingest']['files'][0]['path'])
        taps_path = os.path.join(BASE_DIR, config['ingest']['files'][1]['path'])

        # Cargar ficheros json en DFs
        prints_df = spark.read.json(prints_path)
        taps_df = spark.read.json(taps_path)
        
        # Leer pays en formato CSV con opciones adicionales si existen
        csv_options = config['ingest']['files'][2].get('options', {})

        payments_path = os.path.join(BASE_DIR, config['ingest']['files'][2]['path'])
        payments_df = spark.read.csv(payments_path, **csv_options)

        logging.info("Data ingested successfully.")
        
        return prints_df, taps_df, payments_df

    except FileNotFoundError as fnf_error:
        logging.error(f"File not found: {fnf_error}")
    except AnalysisException as analysis_error:
        logging.error(f"Spark AnalysisException: {analysis_error}")
    except Exception as e:
        logging.error(f"An error occurred during data ingestion: {e}")
        raise
