import duckdb

def get_ids(path: str):
    """
    e.g. get_ids()
    """
    conn = duckdb.connect("./shared/poke_api/bronze.duckdb")
    query = f"""
        SELECT id 
        FROM read_parquet('{path}/bronze*.parquet')
    """
    ids = conn.query(query).fetchdf()["id"]
    print(f'Path used in get_id: {path}')
    
    if ids:
        return ids
    else:
        return "Failed to retrive parquet file"
