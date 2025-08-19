import psycopg2
from config.settings import DB_ADMIN_USER, DB_ADMIN_PWD, DB_APP_USER, DB_APP_PWD, DB_HOST, DB_PORT

def admin_connect(dbname="postgres"):
    return psycopg2.connect(
        dbname=dbname,
        user=DB_ADMIN_USER,
        password=DB_ADMIN_PWD,
        host=DB_HOST,
        port=DB_PORT,
    )

def app_connect(dbname):
    """Connection using app credentials (ideally limited privileges)."""
    return psycopg2.connect(
        dbname=dbname,
        user=DB_APP_USER,
        password=DB_APP_PWD,
        host=DB_HOST,
        port=DB_PORT,
    )
