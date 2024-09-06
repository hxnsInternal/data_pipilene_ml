
# Hans Zamora Carrillo - Data Engineer Test - Mercado Libre

## Descripción

Este proyecto contiene un pipeline de procesamiento de datos en **PySpark**, orquestado mediante **Airflow**. El pipeline realiza las siguientes etapas de procesamiento de datos:
- **Ingesta**: Lectura de archivos de datos en formato JSON y CSV.
- **Transformación**: Limpieza y aplicación de reglas de negocio a los datos.
- **Carga**: Almacenamiento de los datos procesados en un archivo de salida.

El archivo de configuración `config.yaml` permite centralizar las rutas y parámetros clave del pipeline, haciendo que sea fácil de mantener y ajustar.

## Estructura del Proyecto

```
projetc/
├── README.md                      # Archivo README que describe el proyecto
├── airflow/
│   └── dags/
│       └── pipeline_dag.py        # DAG de Airflow que orquesta el pipeline
├── app/
│   ├── configs/
│   │   └── config.yaml            # Archivo de configuración del pipeline
│   ├── data/
│   │   ├── input/                 # Directorio con los archivos de entrada
│   │   │   ├── pays.csv
│   │   │   ├── prints.json
│   │   │   └── taps.json
│   │   └── output/                # Directorio donde se almacenan los archivos de salida
│   ├── logs/                      # Directorio con logs generados por el pipeline
│   ├── scripts/                   # Scripts del pipeline (ingesta, transformación y carga)
│   │   ├── ingest.py
│   │   ├── load.py
│   │   ├── main.py
│   │   └── transform.py
│   └── tests/                     # Pruebas unitarias para cada script del pipeline
└── requirements.txt               # Dependencias del proyecto
```

## Tecnologías

- **PySpark**: Para el procesamiento distribuido de los datos.
- **Apache Airflow**: Orquestación de las tareas del pipeline.
- **YAML**: Para gestionar la configuración del pipeline.

## Requisitos Previos

1. **Python 3.8+**: Asegúrate de tener Python 3.8 o superior instalado.
2. **Apache Airflow**: Instalado y configurado en tu entorno.
3. **PySpark**: Puedes instalarlo usando `pip` o configurar un clúster de Spark.
4. **Docker** (opcional): Para ejecutar Airflow y PySpark en contenedores.

## Instalación

1. **Clonar el repositorio**:

   ```bash
   git clone https://github.com/tu-usuario/py-spark-data-pipeline.git
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
    - path: "../data/input/prints.json"  # Ruta al archivo de impresiones en formato JSON
      format: "json"               # Especifica el formato del archivo (JSON)
    - path: "../data/input/taps.json"    # Ruta al archivo de clics en formato JSON
      format: "json"               # Especifica el formato del archivo (JSON)
    - path: "../data/input/pays.csv"     # Ruta al archivo de pagos en formato CSV
      format: "csv"                # Especifica el formato del archivo (CSV)
      options:                     # Opciones adicionales para la lectura de CSV
        header: true               # Indica si el archivo CSV contiene un encabezado
        inferSchema: true          # Permite a PySpark inferir automáticamente el esquema del archivo

# Configuración de la etapa de transformación de datos
transform:
  time_frame_on_days: 21           # Definición del marco temporal en días para análisis de datos

# Configuración de la etapa de carga de datos procesados
load:
  output_path: "../data/output"    # Directorio de salida donde se almacenarán los datos procesados
  partitions: 1                    # Número de particiones para el archivo de salida
  format: "csv"                    # Formato de salida (CSV)

# Configuración de los logs de la aplicación
logging:
  level: "INFO"                    # Nivel de detalle de los logs (INFO, DEBUG, ERROR, etc.)
  file: "../logs/pipeline.log"     # Archivo donde se almacenarán los logs de la ejecución
```

## Ejecución del Pipeline

### Opción 1: Ejecutar manualmente los scripts

1. **Ingesta de Datos**:

   ```bash
   python app/scripts/ingest.py app/configs/config.yaml
   ```

2. **Transformación de Datos**:

   ```bash
   python app/scripts/transform.py app/configs/config.yaml
   ```

3. **Carga de Datos**:

   ```bash
   python app/scripts/load.py app/configs/config.yaml
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

Gracias por la oportunidad y el tiempo bridnado para desarrollar este test, espero haber demostrado mis capacidades a nivel de python, sql y data engineering en general.
