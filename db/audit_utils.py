def last_upload_for_table(conn, table_name: str):
    """Return last audit row for the table_name or None."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, file_hash, uploaded_at, rows, status FROM upload_audit WHERE table_name = %s ORDER BY uploaded_at DESC LIMIT 1",
            [table_name],
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "file_hash": row[1], "uploaded_at": row[2], "rows": row[3], "status": row[4]}
    finally:
        cur.close()
