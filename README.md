
# Hans Zamora Carrillo - Data Engineer Test 

## Descripción

He utilizado Python, Spark, Spark SQL y Airflow para la construcción del pipeline, respondiendo a todos los requerimientos solicitados en la prueba y demostrando mi dominio en Python y SQL. Además, todo el código y su funcionamiento están debidamente documentados para facilitar su comprensión y lectura. Por último, he implementado buenas prácticas como docstrings, PEP8, modularización del código, pruebas unitarias, despliegue y orquestación.

### Bonus:

Implementé un repositorio en Git para almacenar el pipeline, así como una propuesta de orquestación utilizando Airflow.


Este proyecto contiene un pipeline de procesamiento de datos en **PySpark**, orquestado mediante **Airflow**. El pipeline realiza las siguientes etapas de procesamiento de datos:
- **Ingesta**: Lectura de archivos de datos en formato JSON y CSV.
- **Transformación**: Limpieza y aplicación de reglas de negocio a los datos.
- **Carga**: Almacenamiento de los datos procesados en un archivo de salida.

## Estructura del Proyecto

```
project/
├── README.md                          # Archivo README que describe el proyecto
├── .gitignore                         # Archivos y directorios que Git debe ignorar
├── airflow/
│   └── dags/
│       └── pipeline_dag.py            # DAG de Airflow que orquesta el pipeline
├── app/
│   ├── configs/
│   │   ├── __init__.py                # Inicialización del paquete configs
│   │   └── config.yaml                # Archivo de configuración del pipeline
│   ├── data/
│   │   ├── input/                     # Directorio con los archivos de entrada
│   │   │   ├── pays.csv
│   │   │   ├── prints.json
│   │   │   └── taps.json
│   │   └── output/                    # Directorio donde se almacenan los archivos de salida procesados
│   ├── logs/                          # Directorio con logs generados por el pipeline
│   │   └── __init__.py                # Inicialización del paquete logs
│   ├── scripts/                       # Scripts del pipeline (ingesta, transformación y carga)
│   │   ├── __init__.py                # Inicialización del paquete scripts
│   │   ├── ingest.py                  # Script para la ingesta de datos
│   │   ├── load.py                    # Script para la carga de datos procesados
│   │   ├── main.py                    # Script principal que ejecuta la lógica del pipeline
│   │   └── transform.py               # Script para la transformación de datos
│   └── tests/                         # Pruebas unitarias para cada script del pipeline
│       ├── __init__.py                # Inicialización del paquete tests
│       ├── test_ingest.py             # Pruebas unitarias para ingest.py
│       ├── test_load.py               # Pruebas unitarias para load.py
│       └── test_transform.py          # Pruebas unitarias para transform.py
├── evidences/                         # Directorio con screenshots de la ejecución del pipeline
│   ├── 01 - pipeline_diragram.png     # Diagrama del pipeline
│   ├── 02 - pipeline_execution.png    # Ejecución del pipeline
│   ├── 03 - unit_test_executions.png  # Ejecución de las pruebas unitarias
│   └── 04 - output_pipeline.png       # Output del pipeline
└── requirements.txt                   # Dependencias del proyecto

```

## Tecnologías

- **PySpark**: Para el procesamiento distribuido de los datos.
- **Apache Airflow**: Orquestación de las tareas del pipeline.
- **YAML**: Para gestionar la configuración del pipeline.

## Requisitos Previos

1. **Python 3.8+**: Asegúrate de tener Python 3.8 o superior instalado.
2. **Apache Airflow**: Instalado y configurado en tu entorno.
3. **PySpark**: Puedes instalarlo usando `pip` o configurar un clúster de Spark.

## Instalación

1. **Clonar el repositorio**:

   ```bash
   git clone https://github.com/hxnsInternal/data_pipilene_ml.git
   cd py-spark-data-pipeline
   ```

2. **Crear y activar un entorno virtual** (opcional pero recomendado):

   ```bash
   python3 -m venv env
   source env/bin/activate  # En Windows: env\Scripts\activate
   ```

3. **Instalar las dependencias**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar el archivo de configuración**:
   - Edita el archivo `app/configs/config.yaml` para ajustar las rutas de los archivos de entrada y salida, y cualquier otro parámetro necesario para el pipeline.

## Configuración del Archivo `config.yaml`

El archivo `config.yaml` centraliza todas las configuraciones del pipeline. A continuación, se muestra un ejemplo del archivo actualizado:

```yaml
# Configuración de la etapa de ingestión de datos
ingest:
  files:
    - path: "data/input/prints.json"  # Ruta al archivo de impresiones en formato JSON
      format: "json"                  # Especifica el formato del archivo (JSON)
    - path: "data/input/taps.json"    # Ruta al archivo de clics en formato JSON
      format: "json"                  # Especifica el formato del archivo (JSON)
    - path: "data/input/pays.csv"     # Ruta al archivo de pagos en formato CSV
      format: "csv"                   # Especifica el formato del archivo (CSV)
      options:                        # Opciones adicionales para la lectura de CSV
        header: true                  # Indica si el archivo CSV contiene un encabezado
        inferSchema: true             # Permite a PySpark inferir automáticamente el esquema del archivo

# Configuración de la etapa de transformación de datos
transform:
  time_frame_on_days: 21              # Definición del marco temporal en días para análisis de datos

# Configuración de la etapa de carga de datos procesados
load:
  output_path: "data/output"          # Directorio de salida donde se almacenarán los datos procesados
  partitions: 1                       # Número de particiones para el archivo de salida
  format: "csv"                       # Formato de salida (CSV)

# Configuración de los logs de la aplicación
logging:
  level: "INFO"                       # Nivel de detalle de los logs (INFO, DEBUG, ERROR, etc.)
  file: "logs/pipeline.log"           # Archivo donde se almacenarán los logs de la ejecución
```

## Ejecución del Pipeline

### Opción 1: Ejecutar manualmente los scripts

1. **Ingesta de Datos**:

   ```bash
   python app/scripts/main.py
   ```

### Opción 2: Ejecutar con Airflow

1. **Iniciar Airflow**:
   
   Si tienes Airflow instalado, puedes iniciar el **scheduler** y el **servidor web** con los siguientes comandos:

   ```bash
   airflow scheduler &
   airflow webserver --port 8080
   ```

2. **Verificar el DAG**:
   Abre la interfaz web de Airflow en `http://localhost:8080` y verifica que el DAG `spark_pipeline_dag` esté disponible.

3. **Ejecutar el DAG**:
   Desde la interfaz de Airflow, puedes ejecutar manualmente el DAG o configurarlo para que se ejecute automáticamente.


## Nota

Gracias por la oportunidad y el tiempo brindado para desarrollar este test. Espero haber demostrado mis capacidades y mi capacidad de análisis.