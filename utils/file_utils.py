import re
import hashlib
import io
import pandas as pd
from config.settings import ALLOWED_NAME_RE

def sanitize_name(name: str) -> str:
    """Sanitize user-provided name into allowed lowercase identifier (a-z0-9_)."""
    if not isinstance(name, str):
        raise ValueError("Name must be string")
    name = name.strip().lower().replace(" ", "_")
    name = re.sub(r"[^a-z0-9_]", "", name)
    if not ALLOWED_NAME_RE.match(name):
        raise ValueError(f"Sanitized name '{name}' does not match allowed pattern.")
    return name

def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def df_to_csv_buffer(df: pd.DataFrame) -> io.StringIO:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return buf
