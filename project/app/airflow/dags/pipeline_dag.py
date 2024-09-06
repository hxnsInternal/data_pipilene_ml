from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Definir los argumentos predeterminados para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Pipeline de ETL usando main.py',
    schedule_interval=timedelta(days=1),
) as dag:

    # Definir una única tarea usando BashOperator para ejecutar el script main.py con ruta absoluta
    run_etl = BashOperator(
        task_id='run_main_etl',
        bash_command='python3 /home/user/projetc/app/scripts/main.py',
    )

    run_etl
