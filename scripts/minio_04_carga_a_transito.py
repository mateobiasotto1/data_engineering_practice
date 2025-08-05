# ============================
# Script que carga a bulk
# ============================

import os
import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
load_dotenv()

# Configuracion
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Definiciones
BUCKET_GOLD = "dev-local-gold"

# Conexion a MinIO
try:
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    print(f"Conectando a MinIO en {MINIO_HOST}...")
except Exception as e:
    print(f"Error conectando a MinIO: {e}")
    raise

# Conexion a PostgreSQL
pg_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
engine = create_engine(pg_url)

# Limpia tablas Bulk (ahora con engine.begin(), sin .commit())
try:
    with engine.begin() as conn:
        conn.execute(
            text("TRUNCATE public.bulk_dim_cliente, public.bulk_dim_producto, public.bulk_fact_ventas;")
        )
    print("Tablas bulk limpiadas correctamente antes de la carga.")
except Exception as e:
    print(f"Error al limpiar tablas bulk: {e}")
    raise

# Lee parquet
def leer_parquet(nombre_archivo):
    response = client.get_object(BUCKET_GOLD, nombre_archivo)
    data = response.read()
    return pd.read_parquet(BytesIO(data))

# Carga a Bulk
tablas = {
    "bulk_dim_cliente": "dim_cliente.parquet",
    "bulk_dim_producto": "dim_producto.parquet",
    "bulk_fact_ventas": "fact_ventas.parquet"
}

for tabla_pg, archivo_parquet in tablas.items():
    df = leer_parquet(archivo_parquet)
    print(f"Cargando {archivo_parquet} ({len(df)} filas) a {tabla_pg}...")

# --- Script para Remover duplicados y filas con PK nula ---
    if tabla_pg == "bulk_dim_cliente":
        before = len(df)
        # Eliminar filas con customer_id nulo
        df = df[df["customer_id"].notnull()]
        # Dejar solo la primera aparición por customer_id
        df = df.drop_duplicates(subset=["customer_id"], keep="first")
        after = len(df)
        print(f"   🔹 bulk_dim_cliente: {before} → {after} filas tras limpiar nulos y duplicados en customer_id.")

    elif tabla_pg == "bulk_dim_producto":
        before = len(df)
        df = df[df["product_id"].notnull()]
        df = df.drop_duplicates(subset=["product_id"], keep="first")
        after = len(df)
        print(f"   🔹 bulk_dim_producto: {before} → {after} filas tras limpiar nulos y duplicados en product_id.")

    elif tabla_pg == "bulk_fact_ventas":
        before = len(df)
        df = df[df["order_id"].notnull() & df["product_id"].notnull()]
        df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")

        # Convertir columnas numéricas de 'coma' a 'punto'
        for col in ["sales", "discount", "profit"]:
            if col in df.columns:
                # Reemplaza coma por punto y convierte a float (ignora valores inválidos)
                df[col] = df[col].astype(str).str.replace(",", ".").astype(float)
        after = len(df)
        print(f"   🔹 bulk_fact_ventas: {before} → {after} filas tras limpiar nulos y duplicados en (order_id, product_id).")


    # Carga a la tabla correspondiente
    df.to_sql(tabla_pg, engine, if_exists="append", index=False, schema="public")
    print(f"{len(df)} filas cargadas en {tabla_pg}")