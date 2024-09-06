import sys
import os
import pytest
from pyspark.sql import SparkSession
import shutil

# Añadir el directorio raíz (donde está `projetc`) al path de Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.scripts.load import *

@pytest.fixture(scope="session")
def spark():

    """
    Crea una sesión de Spark con el modo local para las pruebas.
    La sesión es compartida por todas las pruebas en la misma sesión de pytest.

    Returns:
        SparkSession: La sesión de Spark configurada para ejecutarse en modo local.
    """

    return SparkSession.builder.master("local[2]").appName("pytest-pyspark").getOrCreate()

def test_load_data(spark):

    """
    Prueba la función 'load_data' para verificar que los datos se carguen correctamente en un archivo CSV.

    La prueba utiliza un DataFrame de ejemplo y una configuración simulada para la carga.
    Luego, valida que el archivo se haya guardado correctamente y lo elimina después de la verificación.

    Args:
        spark (SparkSession): Sesión de Spark proporcionada por la fixture 'spark'.

    Test:
        - Verifica que el archivo CSV se crea correctamente después de ejecutar la función de carga.

    Asserts:
        - Verifica que el archivo CSV se haya creado en la ruta de salida especificada.

    Cleanup:
        - Elimina el archivo CSV generado después de realizar la verificación.
    """
    
    # Crear un DataFrame de ejemplo
    data = [(1, "prop1", 100), (2, "prop2", 150)]
    columns = ["user_id", "value_prop", "total_spent"]
    df = spark.createDataFrame(data, columns)

    # Configuración simulada para la carga
    config = {
        'load': {
            'output_path': 'data/output/test',
            'partitions': 1,
            'format': 'csv'
        }
    }

    # Ejecutar la carga de datos
    load_data(df, config)

    # Verificar que el archivo se haya guardado correctamente
    assert os.path.exists(config['load']['output_path']), "El archivo de salida no se creó"

    # Eliminar el path que contiene archivo de prueba después de la verificación
    shutil.rmtree(config['load']['output_path']) 
