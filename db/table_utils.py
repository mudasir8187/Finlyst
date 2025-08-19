from psycopg2 import sql

def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table_name,),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()

def get_table_columns(conn, table_name: str):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()

def find_available_table_name(conn, base_name: str) -> str:
    """Return base_name_1 or base_name_2 ... the first non-existing table name."""
    cur = conn.cursor()
    try:
        suffix = 1
        while True:
            candidate = f"{base_name}_{suffix}"
            cur.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name = %s)",
                (candidate,),
            )
            if not cur.fetchone()[0]:
                return candidate
            suffix += 1
    finally:
        cur.close()
