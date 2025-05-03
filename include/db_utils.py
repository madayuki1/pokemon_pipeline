import duckdb

def get_ids(path: str):
    """
    e.g. get_ids()
    """
    conn = duckdb.connect("/shared/poke_api/bronze.duckdb")
    query = f"""
        SELECT id 
        FROM read_parquet('/shared/poke_api/{path}/*.parquet)
    """
    ids = conn.query(query).fetchdf()["id"]
    
    if ids:
        return ids
    else:
        return "Failed to retrive parquet file"
