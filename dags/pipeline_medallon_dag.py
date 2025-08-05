from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'mateo.biasotto',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='pipeline_medallon',
    default_args=default_args,
    description='Pipeline ETL Medallón Bulk → Staging → Public',
    schedule_interval=None, 
    start_date=datetime(2025, 7, 29),
    catchup=False,
    tags=['medallon', 'etl'],
) as dag:

    # Crea esquema y tablas
    crea_esquemas = BashOperator(
        task_id='crea_esquemas',
        bash_command='python3 /opt/airflow/scripts/postgres_01_crea_esquemas.py'
    )

    # Crea Stored Procedure merge_bulk_to_final()
    crea_stored_procedure = BashOperator(
        task_id='crea_stored_procedure',
        bash_command='python3 /opt/airflow/scripts/postgres_02_crea_stored_procedure.py'
    )

    # Crea Vista Analitica
    crea_vista_analitica = BashOperator(
        task_id='crea_vista_analitica',
        bash_command='python3 /opt/airflow/scripts/postgres_03_crea_vista_analitica.py'
    )

    # Carga archivo local a Bronze
    cargar_bronze = BashOperator(
        task_id='cargar_bronze',
        bash_command='python3 /opt/airflow/scripts/minio_01_local_a_bronze.py'
    )

    # Transforma Bronze a Silver
    bronze_a_silver = BashOperator(
        task_id='bronze_a_silver',
        bash_command='python3 /opt/airflow/scripts/minio_02_bronze_a_silver.py'
    )

    # Transforma Silver a Gold
    silver_a_gold = BashOperator(
        task_id='silver_a_gold',
        bash_command='python3 /opt/airflow/scripts/minio_03_silver_a_gold.py'
    )

    # Carga Gold a Bulk 
    cargar_a_bulk = BashOperator(
        task_id='cargar_a_bulk',
        bash_command='python3 /opt/airflow/scripts/minio_04_carga_a_transito.py'
    )

    # Ejecuta merge Bulk → Staging → Public
    ejecutar_merge = PostgresOperator(
        task_id='ejecutar_merge',
        postgres_conn_id='postgres_medallon',
        sql='CALL merge_bulk_to_final();'
    )

    # Ejecucion
    crea_esquemas >> crea_stored_procedure >> crea_vista_analitica
    crea_vista_analitica >> cargar_bronze >> bronze_a_silver >> silver_a_gold >> cargar_a_bulk >> ejecutar_merge
