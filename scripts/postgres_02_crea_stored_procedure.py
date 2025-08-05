# ===============================================
# Script que crea o reemplaza el Stored Procedure
# ===============================================

import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

# Configuracion
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Conexion a PostgreSQL
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()
    print(f"Conectado a PostgreSQL en {POSTGRES_HOST}:5432/{POSTGRES_DB}")
except Exception as e:
    print(f"Error conectando a PostgreSQL: {e}")
    raise

# Ejecutar el script SQL del Stored Procedure
sql_path = "/opt/airflow/sql/03_crear_merge_procedure.sql"
print(f"Ejecutando: {sql_path}")

with open(sql_path, "r", encoding="utf-8") as f:
    script = f.read()

try:
    cur.execute(script)  # Ejecuta todo el bloque SQL
    conn.commit()
    print("Stored Procedure creado/reemplazado correctamente")
except Exception as e:
    print(f"Error ejecutando el Stored Procedure:\n{e}")
    conn.rollback()

cur.close()
conn.close()
