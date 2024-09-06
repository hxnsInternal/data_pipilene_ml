import sys
import os
import pytest
from pyspark.sql import SparkSession

# Añadir el directorio raíz (donde está `projetc`) al path de Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from app.scripts.ingest import *

@pytest.fixture(scope="session")
def spark():
    
    """
    Crea una sesión de Spark con el modo local para las pruebas. 
    La sesión es compartida por todas las pruebas en la misma sesión de pytest.

    Returns:
        SparkSession: La sesión de Spark configurada para ejecutarse en modo local.
    """

    return SparkSession.builder.master("local[2]").appName("pytest-pyspark").getOrCreate()

def test_ingest_data(spark):
    
    """
    Prueba la función 'ingest_data' para asegurarse de que los archivos se ingesten correctamente.
    
    La configuración simulada incluye archivos JSON y CSV, y se utiliza para verificar que los
    datos se carguen correctamente y que los DataFrames resultantes tengan las columnas esperadas.

    Args:
        spark (SparkSession): Sesión de Spark proporcionada por la fixture 'spark'.

    Test:
        - Verifica que los DataFrames de prints, taps y payments no están vacíos.
        - Verifica que las columnas clave existen en cada DataFrame.

    Asserts:
        - Verifica que el DataFrame de prints tenga datos y la columna 'user_id'.
        - Verifica que el DataFrame de taps tenga datos y la columna 'day'.
        - Verifica que el DataFrame de payments tenga datos y la columna 'pay_date'.
    """

    # Configuración simulada
    config = {
        'ingest': {
            'files': [
                {'path': 'data/input/prints.json', 'format': 'json'},
                {'path': 'data/input/taps.json', 'format': 'json'},
                {'path': 'data/input/pays.csv', 'format': 'csv', 'options': {'header': True, 'inferSchema': True}}
            ]   
        }
    }

    # Ejecutar la ingesta
    prints_df, taps_df, payments_df = ingest_data(spark, config)

    # Verificar que los DataFrames no están vacíos
    assert prints_df.count() > 0, "El DataFrame de prints no debería estar vacío"
    assert taps_df.count() > 0, "El DataFrame de taps no debería estar vacío"
    assert payments_df.count() > 0, "El DataFrame de payments no debería estar vacío"

    # Verificar que los esquemas son los correctos
    assert 'user_id' in prints_df.columns, "El DataFrame de prints debería tener la columna user_id"
    assert 'day' in taps_df.columns, "El DataFrame de taps debería tener la columna day"
    assert 'pay_date' in payments_df.columns, "El DataFrame de payments debería tener la columna pay_date"
