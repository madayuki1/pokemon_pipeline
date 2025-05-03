import pandas as pd
import os
import duckdb
import glob

BASE_PATH = "/shared/poke_api"

POKEMON_TABLE = f"{BASE_PATH}/pokemon"
TYPE_TABLE = f"{BASE_PATH}/type"
ABILITY_TABLE = f"{BASE_PATH}/ability"
MOVE_TABLE = f"{BASE_PATH}/move"

TABLES = {"pokemon": POKEMON_TABLE, "type": TYPE_TABLE, "ability": ABILITY_TABLE, "move": MOVE_TABLE}


class DataLoader:
    def __init__(self, dbname: str = "data"):
        self.conn = duckdb.connect(f"/shared/poke_api/{dbname}.duckdb")

    def save_parquet(self, df: pd.DataFrame, path: str, filename: str):
        if df.empty:
            return 
        
        os.makedirs(path, exist_ok=True)
        fullpath = os.path.join(path, f"{filename}.parquet")
        # Register the dataframe
        df.to_parquet(fullpath, index=False, compression='snappy')
        # self.conn.register("temp_df", df)
        # self.conn.query(f"""
        #         COPY temp_df TO '{fullpath}'
        #         (FORMAT PARQUET, COMPRESSION 'SNAPPY', OVERWRITE 1)
        #     """)
        # self.conn.unregister("temp_df")

    def read_parquet(self, path: str, prefix: str = '') -> pd.DataFrame:
        pattern = os.path.join(path, f'{prefix}*.parquet')
        parquet_files = glob.glob(pattern)

        if not parquet_files:
            print('No parquet files found')
            return pd.DataFrame()
        
        df_list = [pd.read_parquet(file) for file in parquet_files]
        return pd.concat(df_list, ignore_index=True)
        # return self.conn.execute(f"SELECT * FROM '{path}/{prefix}*.parquet'").df()

    def get_ids(self, path: str, prefix: str = ''):
        # Check if any parquet files exist
        pattern = os.path.join(path, f'{prefix}*.parquet')
        parquet_files = glob.glob(pattern)
        if not parquet_files:
            print("No parquet files found in path.")
            return pd.Series(dtype=int)
        
        try:
            df_list = [pd.read_parquet(file) for file in parquet_files]
            full_df = pd.concat(df_list, ignore_index=True)

            if 'id' in full_df.columns:
                return full_df['id']
            else:
                print("Column 'id' not found in parquet files")
                return pd.Series(dtype=int)
        except Exception as e:
            print(f"Failed to retrieve parquet files in get_ids: {e}")
            return pd.Series(dtype=int)