import psycopg2
from psycopg2 import Error

def get_db_connection(host, port, dbname, user, password):
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        return conn
    except Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

def list_tables(conn):
    if conn is None:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except Error as e:
        print(f"Error listing tables: {e}")
        return []

def get_table_schema(conn, table_name):
    if conn is None:
        return ""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
        schema = ""
        for col in cursor.fetchall():
            schema += f"{col[0]} {col[1]}\n"
        cursor.close()
        return schema
    except Error as e:
        print(f"Error getting schema for table {table_name}: {e}")
        return ""


