from psycopg2 import sql
from db.connections import admin_connect, app_connect
from utils.file_utils import sanitize_name

def create_user_database(user_id: str) -> str:
    """Create a new database for the sanitized user_id. Safe quoting used."""
    user_id_s = sanitize_name(user_id)
    db_name = f"user_{user_id_s}"
    conn = admin_connect("postgres")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
        if cur.fetchone():
            print(f"Database '{db_name}' already exists.")
        else:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Database '{db_name}' created successfully.")
    finally:
        cur.close()
        conn.close()
    return db_name

def ensure_audit_table(dbname: str):
    """Create upload_audit table if not exists in user's DB."""
    conn = app_connect(dbname)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS upload_audit (
                id serial PRIMARY KEY,
                user_id text,
                erp_name text,
                table_name text,
                file_hash text,
                rows integer,
                action text,
                status text,
                error text,
                uploaded_at timestamptz default now()
            );
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()
