import os
import logging
from pyspark.sql.utils import AnalysisException

def load_data(filtered_df, config: dict):
    
    """
    Guarda el DataFrame filtrado en un archivo CSV utilizando las configuraciones proporcionadas.

    Args:
        filtered_df (DataFrame): El DataFrame de PySpark que contiene los datos filtrados que se deben guardar.
        config (dict): Un diccionario con la configuración de carga que incluye:
            - load['partitions']: Número de particiones para coalesce.
            - load['output_path']: Ruta donde se guardará el archivo CSV.
    
    Raises:
        AnalysisException: Si ocurre un error relacionado con Spark durante la operación de escritura.
        FileNotFoundError: Si la ruta de salida especificada no se encuentra.
        Exception: Cualquier otro error inesperado que pueda ocurrir durante la operación de carga.
    
    Returns:
        None
    """
    
    try:
        logging.info("Saving final DataFrame...")

        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        
        # Guardar el DataFrame en el formato CSV con las configuraciones dadas
        filtered_df.coalesce(config['load']['partitions']).write.csv(
            config['load']['output_path'], header=True, mode="overwrite"
        )
        
        logging.info("Data saved successfully.")

    except AnalysisException as analysis_error:
        logging.error(f"Spark AnalysisException during data load: {analysis_error}")
        raise
    except FileNotFoundError as fnf_error:
        logging.error(f"Output path not found: {fnf_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data loading: {e}")
        raise
