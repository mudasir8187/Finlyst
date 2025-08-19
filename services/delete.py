import os
import pandas as pd
import traceback
from psycopg2 import sql
from config.settings import MAX_UPLOAD_BYTES
from utils.file_utils import sanitize_name, file_sha256, df_to_csv_buffer
from utils.type_mapping import pg_type_from_pd
from db.connections import app_connect
from db.schema_utils import create_user_database, ensure_audit_table
from db.table_utils import table_exists, get_table_columns, find_available_table_name
from db.audit_utils import last_upload_for_table

def delete_erp(user_id: str, erp_name: str):
    user_id_s = sanitize_name(user_id)
    erp_name_s = sanitize_name(erp_name)
    table_name = f"{erp_name_s}"

    dbname = create_user_database(user_id_s)
    ensure_audit_table(dbname)

    conn = app_connect(dbname)
    cur = conn.cursor()

    try:
        # Delete from audit log
        cur.execute(
            sql.SQL("DELETE FROM upload_audit WHERE table_name = %s AND user_id = %s"),
            [table_name, user_id_s]
        )

        # Drop the ERP table
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
        )

        conn.commit()
        print(f"Deleted table '{table_name}' and audit records successfully.")

    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        raise
    finally:
        cur.close()
        conn.close()
