import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, lit, greatest
from pyspark.sql.utils import AnalysisException

def transform_data(prints_df, taps_df, payments_df, spark: SparkSession, config: dict):
    
    """
    Realiza la transformación de los datos de impresiones (prints), taps y pagos, 
    calculando métricas como clics, vistas y montos gastados en intervalos de tiempo previos.

    Args:
        prints_df (DataFrame): DataFrame de PySpark con los datos de impresiones.
        taps_df (DataFrame): DataFrame de PySpark con los datos de taps (clics).
        payments_df (DataFrame): DataFrame de PySpark con los datos de pagos.
        spark (SparkSession): Sesión activa de PySpark.
        config (dict): Diccionario con la configuración de transformación. Debe incluir:
            - transform['time_frame_on_days']: Número de días para calcular métricas históricas (vistas, clics, pagos).

    Returns:
        DataFrame: Un DataFrame con las métricas calculadas para cada impresión, incluyendo vistas, clics, pagos y montos gastados en un intervalo de tiempo.

    Raises:
        AnalysisException: Si ocurre un error relacionado con Spark durante las consultas o transformaciones.
        ValueError: Si no se encuentran fechas válidas en los datos de impresiones.
        Exception: Cualquier otro error inesperado durante la transformación de los datos.
    """

    try:
        logging.info("Starting data transformation...")

        # Convertir columnas de fecha a tipo Date
        prints_df = prints_df.withColumn("day", col("day").cast("date"))
        taps_df = taps_df.withColumn("day", col("day").cast("date"))
        payments_df = payments_df.withColumn("pay_date", col("pay_date").cast("date"))

        logging.info("Data transformation: ok date casting")

        # Registrar las tablas como vistas temporales
        prints_df.createOrReplaceTempView("prints_table")
        taps_df.createOrReplaceTempView("taps_table")
        payments_df.createOrReplaceTempView("payments_table")

        logging.info("Data transformation: ok temp tables (prints, taps and payments)")

        # Calcular la última fecha en los datos
        max_date = prints_df.agg({"day": "max"}).collect()[0][0]

        if not max_date:
            raise ValueError("No valid dates found in the prints data.")

        # Filtrar los prints de la última semana
        last_week_prints_df = prints_df.filter(col('day') >= date_sub(lit(max_date), 7))
        last_week_prints_df.createOrReplaceTempView("last_week_prints")

        logging.info("Data transformation: ok last_week_prints table")

        # Consulta SQL para determinar si hubo un click para cada print
        click_query = """
            with taps as (
                select user_id, 
                    event_data.value_prop, 
                    min(day) click_day
                from taps_table
                    group by 1,2 
            )
                select p.*,
                if(t.user_id is not null, 1, 0) clicked
                from last_week_prints p
                left join taps t on p.user_id = t.user_id 
                    and p.event_data.value_prop = t.value_prop 
                    and t.click_day >= p.day
        """

        clicks_df = spark.sql(click_query)
        clicks_df.createOrReplaceTempView("clicks_table")

        logging.info("Data Transformation: ok clicks_table table")

        time_frame = config['transform']['time_frame_on_days']

        # Consulta SQL para calcular los clicks, vistas, pagos y montos en las tres semanas anteriores
        previous_data_query = f"""
            select p.day,
                p.user_id,
                p.event_data.value_prop,
                p.clicked,
                count(distinct v.day) view_count,
                count(distinct c.day) click_count,
                count(distinct pay.pay_date) payment_count,
                round(coalesce(sum(pay.total), 0), 2) total_spent
            from clicks_table p 
            left join prints_table v on p.user_id = v.user_id 
                and p.event_data.value_prop = v.event_data.value_prop 
                and v.day >= date_sub(p.day, {time_frame}) 
                and v.day < p.day
            left join taps_table c on p.user_id = c.user_id 
                and p.event_data.value_prop = c.event_data.value_prop 
                and c.day >= date_sub(p.day, {time_frame}) 
                and c.day < p.day
            left join payments_table pay on p.user_id = pay.user_id 
                and p.event_data.value_prop = pay.value_prop 
                and pay.pay_date >= date_sub(p.day, {time_frame}) 
                and pay.pay_date < p.day
            group by 1,2,3,4
            order by 1 desc
        """

        final_df = spark.sql(previous_data_query)

        # Filtrar los registros donde todas las métricas son cero
        final_df = final_df.filter(greatest(
            col("view_count"),
            col("click_count"),
            col("payment_count"),
            col("total_spent")
        ) > 0)

        logging.info("Data transformation completed.")

        return final_df

    except AnalysisException as analysis_error:
        logging.error(f"Spark AnalysisException during transformation: {analysis_error}")
        raise
    except ValueError as val_error:
        logging.error(f"Data validation error: {val_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data transformation: {e}")
        raise
