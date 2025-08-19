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


def upload_erp_data(user_id: str, erp_name: str, file_path: str):
    """
    Upload ERP Excel/CSV data into the user's dedicated Postgres DB.
    Creates DB and table if not present, adds audit logs, and handles schema changes.
    """

    # -------- 1. Basic validations --------
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File '{file_path}' not found.")

    file_size = os.path.getsize(file_path)
    if file_size > MAX_UPLOAD_BYTES:
        raise ValueError(f"File too large: {file_size} bytes (max {MAX_UPLOAD_BYTES} bytes).")

    user_id_s = sanitize_name(user_id)
    erp_name_s = sanitize_name(erp_name)
    table_name = f"{erp_name_s}"

    # -------- 2. Load file into DataFrame --------
    if file_path.lower().endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Only CSV, XLS, XLSX files are supported.")

    if df.empty:
        raise ValueError("Uploaded file is empty.")

    # -------- 3. Create user database if needed --------
    dbname = create_user_database(user_id_s)

    # -------- 4. Ensure audit table exists --------
    ensure_audit_table(dbname)

    # -------- 5. Connect to DB --------
    conn = app_connect(dbname)
    cur = conn.cursor()

    try:
        # -------- 6. Check table existence and schema changes --------
        if table_exists(conn, table_name):
            existing_cols = get_table_columns(conn, table_name)
            new_cols = list(df.columns)

            if existing_cols != new_cols:
                print(f"Schema change detected for table '{table_name}'. Creating new version.")
                table_name = find_available_table_name(conn, table_name)

        # -------- 7. Create table if not exists --------
        if not table_exists(conn, table_name):
            col_defs = [
                sql.SQL("{} {}").format(
                    sql.Identifier(c),
                    sql.SQL(pg_type_from_pd(df[c].dtype))
                )
                for c in df.columns
            ]
            create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(col_defs)
            )
            cur.execute(create_stmt)
            conn.commit()

        # -------- 8. File hash check --------
        file_hash = file_sha256(file_path)
        last_audit = last_upload_for_table(conn, table_name)
        if last_audit and last_audit["file_hash"] == file_hash:
            print(f"No changes since last upload for '{table_name}'. Skipping insert.")
            return

        # -------- 9. Insert data --------
        buf = df_to_csv_buffer(df)
        cur.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(table_name)),
            buf
        )
        conn.commit()

        # -------- 10. Log success in audit --------
        cur.execute(
            """
            INSERT INTO upload_audit (user_id, erp_name, table_name, file_hash, rows, action, status)
            VALUES (%s, %s, %s, %s, %s, 'upload', 'success')
            """,
            (user_id_s, erp_name_s, table_name, file_hash, len(df)),
        )
        cur.execute(
            sql.SQL("""
                DO $$
                DECLARE
                    tbl_name   TEXT := {tbl};
                    col_list   TEXT;
                    sql_query  TEXT;
                BEGIN
                    SELECT string_agg(quote_ident(column_name) || ' IS NULL', ' OR ')
                    INTO col_list
                    FROM information_schema.columns
                    WHERE table_name = tbl_name
                    AND table_schema = 'public';

                    IF col_list IS NOT NULL THEN
                        sql_query := format('DELETE FROM %I WHERE %s', tbl_name, col_list);
                        EXECUTE sql_query;
                    END IF;
                END $$;
            """).format(
                tbl=sql.Literal(erp_name_s)  
            )
        )
        conn.commit()
        cur.execute(sql.SQL("""
                    DO $$
                    DECLARE
                        col_list TEXT;
                        tbl_name TEXT := {tbl};
                    BEGIN
                        SELECT string_agg(quote_ident(column_name), ', ')
                        INTO col_list
                        FROM information_schema.columns
                        WHERE table_name = tbl_name
                        AND column_name <> 'ctid';

                        IF col_list IS NOT NULL THEN
                            EXECUTE format(
                                'DELETE FROM %I WHERE ctid NOT IN (SELECT MIN(ctid) FROM %I GROUP BY %s)',
                                tbl_name, tbl_name, col_list
                            );
                        END IF;
                    END$$;
                """).format(
                    tbl=sql.Literal(erp_name_s)
                ))


        conn.commit()

        print(f"Upload complete: {len(df)} rows inserted into '{table_name}'.")

    except Exception as e:
        conn.rollback()
        # Log failure in audit
        try:
            cur.execute(
                """
                INSERT INTO upload_audit (user_id, erp_name, table_name, file_hash, rows, action, status, error)
                VALUES (%s, %s, %s, %s, %s, 'upload', 'failure', %s)
                """,
                (user_id_s, erp_name_s, table_name, None, len(df) if not df.empty else 0, str(e)),
            )
            cur.execute(
                sql.SQL("""
                    DO $$
                    DECLARE
                        tbl_name   TEXT := {tbl};
                        col_list   TEXT;
                        sql_query  TEXT;
                    BEGIN
                        SELECT string_agg(quote_ident(column_name) || ' IS NULL', ' OR ')
                        INTO col_list
                        FROM information_schema.columns
                        WHERE table_name = tbl_name
                        AND table_schema = 'public';

                        IF col_list IS NOT NULL THEN
                            sql_query := format('DELETE FROM %I WHERE %s', tbl_name, col_list);
                            EXECUTE sql_query;
                        END IF;
                    END $$;
                """).format(
                    tbl=sql.Literal(erp_name_s + "_data")  
                )
            )
            conn.commit()
            cur.execute(sql.SQL("""
                        DO $$
                        DECLARE
                            col_list TEXT;
                            tbl_name TEXT := {tbl};
                        BEGIN
                            SELECT string_agg(quote_ident(column_name), ', ')
                            INTO col_list
                            FROM information_schema.columns
                            WHERE table_name = tbl_name
                            AND column_name <> 'ctid';

                            IF col_list IS NOT NULL THEN
                                EXECUTE format(
                                    'DELETE FROM %I WHERE ctid NOT IN (SELECT MIN(ctid) FROM %I GROUP BY %s)',
                                    tbl_name, tbl_name, col_list
                                );
                            END IF;
                        END$$;
                    """).format(
                        tbl=sql.Literal(erp_name_s + "_data")
                    ))


            conn.commit()


            conn.commit()
        except Exception:
            conn.rollback()
        traceback.print_exc()
        raise
    finally:
        cur.close()
        conn.close()
