# ==========================================================
# Script que transforma archivo de capa Silver a capa Gold
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
BUCKET_SILVER = "dev-local-silver"
BUCKET_GOLD = "dev-local-gold"
OBJECT_NAME_SILVER = "data_sales_silver.parquet"

# Conexion a MinIO
try:
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    print(f"Conectando a MinIO en {MINIO_HOST}...") 

    # Descarga el Parquet desde Silver
    if not client.bucket_exists(BUCKET_SILVER):
        raise Exception(f"El bucket '{BUCKET_SILVER}' no existe en MinIO")

    print(f"Descargando '{OBJECT_NAME_SILVER}' desde bucket '{BUCKET_SILVER}'...")
    response = client.get_object(BUCKET_SILVER, OBJECT_NAME_SILVER)
    data = response.read()
    buffer = BytesIO(data)

    # Lee el Parquet
    df = pd.read_parquet(buffer)
    print(f"Datos cargados desde Silver: {df.shape[0]} filas, {df.shape[1]} columnas")

    # Crea dimension Cliente
    dim_cliente = df[[
        "customer_id", "customer_name", "segment",
        "country", "city", "state", "postal_code", "region"
    ]].drop_duplicates().reset_index(drop=True)
    print(f"dim_cliente: {dim_cliente.shape[0]} filas")

    # Crea dimension Producto
    dim_producto = df[[
        "product_id", "product_name", "category", "sub_category"
    ]].drop_duplicates().reset_index(drop=True)
    print(f"dim_producto: {dim_producto.shape[0]} filas")

    # Crea tabla Ventas
    fact_ventas = df[[
        "order_id", "order_date", "ship_date", "customer_id", "product_id",
        "sales", "quantity", "discount", "profit"
    ]].reset_index(drop=True)
    print(f"fact_ventas: {fact_ventas.shape[0]} filas")

    # Guarda el bucket en Gold
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    def guardar_parquet(df_local, nombre_archivo):
        parquet_buffer = BytesIO()
        df_local.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        client.put_object(
            BUCKET_GOLD,
            nombre_archivo,
            data=parquet_buffer,
            length=len(parquet_buffer.getvalue()),
            content_type="application/octet-stream"
        )
        print(f"Guardado en Gold: {nombre_archivo}")

    guardar_parquet(dim_cliente, "dim_cliente.parquet")
    guardar_parquet(dim_producto, "dim_producto.parquet")
    guardar_parquet(fact_ventas, "fact_ventas.parquet")

except S3Error as e:
    print(f"Error de MinIO: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")
