def pg_type_from_pd(dtype) -> str:
    """Basic mapping from pandas dtype to Postgres type. Extend as needed."""
    dt = str(dtype)
    if "int" in dt:
        return "bigint"
    if "float" in dt:
        return "double precision"
    if "bool" in dt:
        return "boolean"
    if "datetime" in dt:
        return "timestamptz"
    return "text"
