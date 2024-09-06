from datetime import datetime
import logging
import yaml
from pyspark.sql import SparkSession
from ingest import *
from transform import *
from load import *

def setup_logging(config):
    
    """
    Configura el sistema de logging, generando un archivo de log con la fecha y hora actuales.

    Args:
        config (dict): Diccionario que contiene la configuración del logging, específicamente:
            - logging['file']: Ruta base del archivo de log.
            - logging['level']: Nivel de logging (INFO, DEBUG, ERROR, etc.).

    Raises:
        KeyError: Si falta alguna clave necesaria en la configuración de logging.
        Exception: Si ocurre algún error inesperado al configurar el logging.
    """

    try:
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_filename = f"{config['logging']['file'].split('.log')[0]}_{current_time}.log"

        logging.basicConfig(
            filename=log_filename,
            level=getattr(logging, config['logging']['level']),
            format='%(asctime)s %(levelname)s %(message)s'
        )
    except KeyError as key_error:
        logging.error(f"Logging configuration key missing: {key_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while setting up logging: {e}")
        raise

def load_config(config_path):
    """
    Carga el archivo de configuración YAML.

    Args:
        config_path (str): Ruta al archivo YAML de configuración.

    Returns:
        dict: Diccionario que representa el contenido del archivo de configuración YAML.

    Raises:
        FileNotFoundError: Si el archivo de configuración no se encuentra.
        yaml.YAMLError: Si ocurre un error al analizar el archivo YAML.
        Exception: Cualquier otro error inesperado al cargar la configuración.
    """

    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError as fnf_error:
        logging.error(f"Configuration file not found: {fnf_error}")
        raise
    except yaml.YAMLError as yaml_error:
        logging.error(f"Error parsing YAML configuration: {yaml_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading the configuration: {e}")
        raise

def create_spark_session(app_name):
    """
    Crea y devuelve una sesión de Spark.

    Args:
        app_name (str): Nombre de la aplicación para la sesión de Spark.

    Returns:
        SparkSession: La sesión activa de Spark.

    Raises:
        SparkSession: Si ocurre un error relacionado con la creación de la sesión de Spark.
        Exception: Cualquier otro error inesperado al crear la sesión de Spark.
    """

    try:
        return SparkSession.builder.appName(app_name).getOrCreate()
    except SparkSession as spark_error:
        logging.error(f"Failed to create Spark session: {spark_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while creating Spark session: {e}")
        raise

if __name__ == "__main__":
    """
    Función principal que ejecuta el pipeline de ingesta, transformación y carga de datos.
    
    Pasos:
        1. Cargar la configuración desde un archivo YAML.
        2. Configurar el sistema de logging.
        3. Crear una sesión de Spark.
        4. Ingestar datos desde archivos JSON y CSV.
        5. Transformar los datos y calcular métricas clave.
        6. Cargar los datos transformados a un archivo CSV.
    
    Logs:
        Los errores y eventos se registran en un archivo de log generado dinámicamente con la fecha y hora de la ejecución.
    
    Raises:
        Exception: Cualquier error inesperado durante la ejecución del pipeline.
    """

    try:
        # Cargar configuración
        config = load_config('../configs/config.yaml')
        
        # Configurar logging
        setup_logging(config)
        
        # Crear sesión de Spark
        spark = create_spark_session("ClicksAnalysis")
        
        logging.info("Starting pipeline...")
        
        # Ingesta de datos
        prints_df, taps_df, payments_df = ingest_data(spark, config)

        # Transformación de datos
        transformed_df = transform_data(prints_df, taps_df, payments_df, spark, config)
        
        # transformed_df.show(10)

        # Carga de datos
        load_data(transformed_df, config)
        
        logging.info("Pipeline completed successfully.")
    
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")
        raise
