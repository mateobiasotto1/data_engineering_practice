# ===============================================
# Script que crea las tablas del esquema public
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

# Ejecuta el script SQL
sql_path = "/opt/airflow/sql/01_crear_tablas.sql"
print(f"Ejecutando: {sql_path}")

with open(sql_path, "r") as f:
    script = f.read()

# Ejecuta sentencia SQL
for statement in script.split(";"):
    stmt = statement.strip()
    if stmt:
        try:
            cur.execute(stmt + ";")
        except Exception as e:
            print(f"Error en la sentencia:\n{stmt}\n{e}")

conn.commit()
cur.close()
conn.close()

print("Script ejecutado correctamente")
