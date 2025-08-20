import os
import re
import streamlit as st
# ---------- Configuration (use environment variables) ----------
DB_HOST = "123.45.67.89"  #st.secrets["db_host"] #os.getenv("DB_HOST", "localhost")
DB_PORT = 5432 #st.secrets["db_port"]  #os.getenv("DB_PORT", "5432")
DB_ADMIN_USER = st.secrets["db_user"]  #os.getenv("DB_ADMIN_USER", "postgres")
DB_ADMIN_PWD = st.secrets["db_password"]  # os.getenv("DB_ADMIN_PWD", "moni123")

# Optionally override DB_USER/PWD for tenant DB connections (least privilege)
DB_APP_USER = st.secrets["db_user"] #os.getenv("DB_APP_USER", DB_ADMIN_USER)
DB_APP_PWD = st.secrets["db_password"] #os.getenv("DB_APP_PWD", DB_ADMIN_PWD)

# Limits
MAX_UPLOAD_BYTES = 200 * 1024 * 1024 #int(os.getenv("MAX_UPLOAD_BYTES", 200 * 1024 * 1024))
ALLOWED_NAME_RE = re.compile(r"^[a-z0-9_]+$")

