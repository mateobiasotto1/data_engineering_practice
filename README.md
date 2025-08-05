# Proyecto de Arquitectura Medallón con Airflow, MinIO, PostgreSQL y Python

Este proyecto implementa una arquitectura Medallón para el procesamiento de datos de operaciones de ventas, utilizando Docker Compose para la orquestación de servicios y Python con Jupyter Notebook para el desarrollo de scripts de transformación. El flujo de trabajo se automatiza con Apache Airflow.

## Descripción General

El objetivo principal es leer datos de un archivo CSV o JSON de operaciones de ventas, procesarlos en diferentes capas (Bronze, Silver, Gold) utilizando la arquitectura Medallón, y finalmente cargarlos en una base de datos PostgreSQL para análisis y visualización.

## Tecnologías Utilizadas

  **Docker Compose:** Para la orquestación de servicios (PostgreSQL, MinIO, Airflow).
  **Python:** Para el desarrollo de scripts de procesamiento de datos.
  **Jupyter Notebook:** Para el desarrollo y experimentación de los scripts en un entorno local.
  **MinIO:** Almacenamiento de objetos compatible con S3, utilizado como Data Lake para las capas Bronze, Silver y Gold.
  **PostgreSQL:** Base de datos relacional para almacenar los datos transformados en la capa Gold.
  **Apache Airflow:** Orquestador de flujos de trabajo para automatizar la ejecución de los scripts.
  **Parquet:** Formato de almacenamiento columnar eficiente para datos en la capa Silver y Gold.

## Arquitectura Medallón

El proyecto sigue la arquitectura Medallón, que se divide en tres capas principales:

  **Bronze:** Almacenamiento de los datos originales, sin procesar, provenientes del archivo CSV/JSON en MinIO (bucket dev-local-bronze).
  **Silver:** Datos transformados y limpios, almacenados en formato Parquet en MinIO (bucket dev-local-silver).
  **Gold:** Datos agregados y modelados para el análisis, almacenados en formato Parquet en MinIO (bucket dev-local-gold) y cargados en la base de datos PostgreSQL.

## Configuración del Entorno

1.  **Docker Compose:**

    *   Crear un archivo docker-compose.yml que defina los servicios de PostgreSQL, MinIO y Airflow. Asegurarse de configurar las variables de entorno necesarias para cada servicio (credenciales, puertos, etc.).
    *   Ejecutar docker-compose up -d para iniciar los servicios.

2.  **Python y Jupyter Notebook:**

    *   Crear un entorno virtual Python.
    *   Instalar las dependencias necesarias: pandas, pyarrow, minio, psycopg2-binary, etc. (Se puede usar un archivo requirements.txt).
    *   Iniciar Jupyter Notebook.

## Scripts de Procesamiento de Datos

Los siguientes scripts deben ser desarrollados en Python utilizando Jupyter Notebook.

1.  **Carga de Datos a Bronze:**

    *   Leer el archivo CSV/JSON de operaciones de ventas en local.
    *   Conectarse al cliente MinIO.
    *   Cargar el archivo CSV/JSON en el bucket dev-local-bronze.

2.  **Transformación Bronze a Silver:**

    *   Conectarse al cliente MinIO.
    *   Leer el archivo CSV del bucket dev-local-bronze.
    *   Realizar transformaciones de limpieza y estandarización de datos. Por ejemplo, convertir tipos de datos, manejar valores faltantes, etc.
    *   Guardar los datos transformados en formato Parquet en el bucket dev-local-silver.

3.  **Transformación Silver a Gold:**

    *   Conectarse al cliente MinIO.
    *   Leer el archivo Parquet del bucket dev-local-silver.
    *   Realizar transformaciones de agregación y modelado para crear las dimensiones y la tabla de hechos.
        *   dim_inventario: Información sobre los productos vendidos.
        *   dim_cliente: Información sobre los clientes que realizaron las compras.
        *   fact_operaciones_ventas: Datos de las ventas, incluyendo claves foráneas a las dimensiones.
    *   Guardar las dimensiones y la tabla de hechos como archivos Parquet separados en el bucket dev-local-gold.

4.  **Carga de Datos a PostgreSQL (Tabla de Tránsito):**

    *   Conectarse al cliente MinIO.
    *   Leer los archivos Parquet de las dimensiones y la tabla de hechos del bucket dev-local-gold.
    *   Conectarse a la base de datos PostgreSQL (asegurarse de haber creado la base de datos manualmente).
    *   Crear tablas de tránsito (bulk tables) en el esquema public para cada dimensión y la tabla de hechos. (Ej: bulk_dim_inventario, bulk_dim_cliente, bulk_fact_operaciones_ventas).
    *   Cargar los datos de los DataFrames a las tablas de tránsito utilizando la función to_sql de Pandas. Usar el método append para agregar los datos.

    **Importante:** Asegúrate de crear las tablas de tránsito en la base de datos PostgreSQL antes de ejecutar este script. La estructura de las tablas debe coincidir con la estructura de los DataFrames que se van a cargar.

5.  **Stored Procedure para Merge de Datos:**

    *   Desarrollar un stored procedure en PostgreSQL que realice el merge de los datos de las tablas de tránsito (esquema public) a las tablas de dimensiones y la tabla de hechos finales (en un esquema diferente, por ejemplo, staging o reporting).
    *   El stored procedure debe manejar la inserción de nuevos registros y la actualización de registros existentes. Utilizar la sentencia MERGE (o INSERT ... ON CONFLICT DO UPDATE si la versión de PostgreSQL es anterior a 15).

    **Importante:** Adaptar el stored procedure a la estructura de tus tablas y a la lógica de negocio específica para la actualización de datos. Considerar el manejo de errores y la optimización del rendimiento. Crear el schema staging (o el que elijas) y las tablas destino (staging.dim_inventario, staging.dim_cliente, staging.fact_operaciones_ventas) antes de ejecutar el stored procedure.

6.  **Vista de Datos:**

    *   Crear una vista en PostgreSQL que combine datos de las dimensiones y la tabla de hechos en el esquema staging (o el esquema donde se encuentran las tablas finales) para facilitar el análisis y la consulta de datos.

7.  **DAG de Airflow:**

    *   Crear un DAG en Airflow que automatice la ejecución de los scripts de Python y el stored procedure de PostgreSQL.
    *   El DAG debe incluir las siguientes tareas:
        *   Tarea para ejecutar el script de carga de datos a Bronze.
        *   Tarea para ejecutar el script de transformación Bronze a Silver.
        *   Tarea para ejecutar el script de transformación Silver a Gold.
        *   Tarea para ejecutar el script de carga de datos a PostgreSQL (tabla de tránsito).
        *   Tarea para ejecutar el stored procedure de merge de datos.

    **Importante:** Asegúrate de configurar las conexiones a MinIO y PostgreSQL en la interfaz de usuario de Airflow. Reemplaza las rutas de los scripts con las rutas reales en tu sistema. Considera el uso de variables de Airflow para almacenar las credenciales y otros parámetros de configuración.

