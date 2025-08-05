# ======================================
# script que sube archivio local a Bronze
# ======================================

import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
load_dotenv()

# Configuracion
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")

# Definiciones
BUCKET_NAME = "dev-local-bronze"
LOCAL_FILE = "/opt/airflow/scripts/data.csv"
OBJECT_NAME = "data_sales.csv"                  

# Conexion a MinIO
try:
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    print(f"Conectando a MinIO en {MINIO_HOST} ...")

    # Crea bucket si no existe
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' creado")
    else:
        print(f"Bucket '{BUCKET_NAME}' ya existe")

    # Subir archivo a Bronze
    client.fput_object(BUCKET_NAME, OBJECT_NAME, LOCAL_FILE)
    print(f"Archivo '{LOCAL_FILE}' subido como '{OBJECT_NAME}' a bucket '{BUCKET_NAME}'")

except S3Error as e:
    print(f"Error con MinIO: {e}")
except Exception as e:
    print(f"Error: {e}")

