# ==========================================================
# Script que transforma archivo de capa Bronze a capa Silver
# ==========================================================

import os
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from dotenv import load_dotenv
load_dotenv()

# Configuracion
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")

# Definiciones
BUCKET_BRONZE = "dev-local-bronze"
BUCKET_SILVER = "dev-local-silver"
OBJECT_NAME = "data_sales.csv"
PARQUET_NAME = "data_sales_silver.parquet"

# Conexion a MinIO
try:
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    print(f"Conectando a MinIO en {MINIO_HOST}...")

    # Descarga el CSV desde Bronze

    if not client.bucket_exists(BUCKET_BRONZE):
        raise Exception(f"El bucket '{BUCKET_BRONZE}' no existe en MinIO")

    print(f"Descargando '{OBJECT_NAME}' desde bucket '{BUCKET_BRONZE}'...")
    response = client.get_object(BUCKET_BRONZE, OBJECT_NAME)
    data = response.read()
    buffer = BytesIO(data)

    # Procesa los datos
    df = pd.read_csv(buffer, sep=";", quotechar='"', engine="python", on_bad_lines="skip")
    print(f"Datos originales: {df.shape[0]} filas, {df.shape[1]} columnas")

    # Normaliza nombres de columnas
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    print(f"Columnas encontradas: {df.columns.tolist()}")

    # Columnas necesarias
    cols_needed = ["order_date", "order_id"]

    # Valida columnas y limpiar datos
    missing = [col for col in cols_needed if col not in df.columns]
    if missing:
        print(f"Columnas faltantes: {missing}. No se filtrarán esas columnas.")
    else:
        df.dropna(subset=cols_needed, inplace=True)
        df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True, errors="coerce")
        if "ship_date" in df.columns:
            df["ship_date"] = pd.to_datetime(df["ship_date"], dayfirst=True, errors="coerce")


    # Guarda Parquet en Silver
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    client.put_object(
        BUCKET_SILVER,
        PARQUET_NAME,
        data=parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    print(f"Datos limpios cargados en Silver como '{PARQUET_NAME}'")

except S3Error as e:
    print(f"Error de MinIO: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")
