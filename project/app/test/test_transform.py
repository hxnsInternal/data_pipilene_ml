import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

# Añadir el directorio raíz (donde está `projetc`) al path de Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.scripts.transform import *

@pytest.fixture(scope="session")
def spark():
    
    """
    Crea una sesión de Spark con el modo local para las pruebas. 
    La sesión es compartida por todas las pruebas en la misma sesión de pytest.

    Returns:
        SparkSession: La sesión de Spark configurada para ejecutarse en modo local.
    """

    return SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()

def test_transform_data_success(spark):
    
    """
    Prueba que la función transform_data realice correctamente la transformación de datos
    y calcule las métricas de clics, vistas, pagos y montos.
    """

    # Crear datos de prueba para impresiones, taps y pagos
    prints_data = [
        Row(user_id=1, day="2024-08-25", event_data=Row(value_prop="A")),
        Row(user_id=1, day="2024-08-26", event_data=Row(value_prop="A")),
        Row(user_id=2, day="2024-08-26", event_data=Row(value_prop="B"))
    ]
    
    taps_data = [
        Row(user_id=1, day="2024-08-25", event_data=Row(value_prop="A")),
        Row(user_id=2, day="2024-08-26", event_data=Row(value_prop="B"))
    ]
    
    payments_data = [
        Row(user_id=1, pay_date="2024-08-24", value_prop="A", total=100.0),
        Row(user_id=2, pay_date="2024-08-25", value_prop="B", total=150.0)
    ]

    prints_df = spark.createDataFrame(prints_data)
    taps_df = spark.createDataFrame(taps_data)
    payments_df = spark.createDataFrame(payments_data)

    # Configuración de prueba
    config = {
        'transform': {
            'time_frame_on_days': 21  # Usar las últimas 3 semanas
        }
    }

    # Ejecutar la transformación de datos
    result_df = transform_data(prints_df, taps_df, payments_df, spark, config)

    # Verificar que el DataFrame resultante no está vacío
    assert result_df.count() > 0

    # Verificar que las columnas esperadas están presentes
    expected_columns = {'day', 'user_id', 'value_prop', 'clicked', 'view_count', 'click_count', 'payment_count', 'total_spent'}
    assert set(result_df.columns) == expected_columns

    # Verificar que se hayan calculado correctamente los valores de la métrica clicked
    result = result_df.collect()
    assert result[0]["clicked"] == 1  # Usuario 1 hizo clic en la prop "A"
    assert result[1]["clicked"] == 0  # Usuario 2 no hizo clic en la prop "B"

def test_transform_data_no_valid_dates(spark):
    """
    Prueba que la función transform_data arroje un ValueError si no hay fechas válidas en los datos de impresiones.
    """
    # Crear DataFrames vacíos o con fechas inválidas
    prints_df = spark.createDataFrame([], schema="user_id INT, day STRING, event_data STRUCT<value_prop: STRING>")
    taps_df = spark.createDataFrame([], schema="user_id INT, day STRING, event_data STRUCT<value_prop: STRING>")
    payments_df = spark.createDataFrame([], schema="user_id INT, pay_date STRING, value_prop STRING, total DOUBLE")

    config = {
        'transform': {
            'time_frame_on_days': 21
        }
    }

    # Verificar que la función arroje un ValueError
    with pytest.raises(ValueError, match="No valid dates found in the prints data."):
        transform_data(prints_df, taps_df, payments_df, spark, config)

def test_transform_data_analysis_exception(spark):
    """
    Prueba que la función transform_data arroje un AnalysisException si hay un error en la consulta de Spark.
    """
    # Crear un DataFrame de impresiones con columnas incorrectas para simular un error
    prints_data = [Row(user_id=1, invalid_column="2024-08-25", event_data=Row(value_prop="A"))]
    prints_df = spark.createDataFrame(prints_data)

    taps_df = spark.createDataFrame([], schema="user_id INT, day STRING, event_data STRUCT<value_prop: STRING>")
    payments_df = spark.createDataFrame([], schema="user_id INT, pay_date STRING, value_prop STRING, total DOUBLE")

    config = {
        'transform': {
            'time_frame_on_days': 21
        }
    }

    # Verificar que la función arroje un AnalysisException
    with pytest.raises(AnalysisException):
        transform_data(prints_df, taps_df, payments_df, spark, config)
