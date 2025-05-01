import duckdb
import os

def get_ids(path: str):
        """
        e.g. get_ids()
        """
        conn = duckdb.connect(f"/shared/poke_api/bronze.duckdb")
        ids = conn.query(f"""
                                SELECT id 
                                FROM read_parquet('/shared/poke_api/{path}/*.parquet)
                            """).fetchdf()['id']
        if ids:
            return ids
        else:
            return "Failed to retrive parquet file"