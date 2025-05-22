import pandas as pd
import os
import duckdb
import glob
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime

class DataLoader:
    def __init__(self, path: str, layer: str = "data"):
        self.path = path
        self.layer = layer

    def save_parquet(self, df: pd.DataFrame, table_name:str, filename: str=''):
        if df.empty:
            return 
        
        now = datetime.now(ZoneInfo("Asia/Singapore"))
        if filename == '':
            filename = f'{table_name}_{now.strftime("%Y-%m-%d__%H-%M-%S")}.parquet'
        else:
            filename = f'{filename}.parquet'

        table_path = Path(self.path) / self.layer
        os.makedirs(table_path, exist_ok=True)

        file_path = table_path / filename
        df.to_parquet(file_path, index=False, compression='snappy')

    def read_parquet(self, table_name) -> pd.DataFrame:
        pattern = Path(self.path) / self.layer / f'{table_name}*.parquet'
        parquet_files = glob.glob(str(pattern))

        if not parquet_files:
            print(f"Resolved base: {Path(self.path).resolve()}")
            print(f"Pattern used: {pattern}")
            print(f"Matching files: {glob.glob(str(pattern))}")
            print('No parquet files found')
            return pd.DataFrame()
        
        df_list = [pd.read_parquet(file) for file in parquet_files]
        return pd.concat(df_list, ignore_index=True)

    def get_ids(self, table_name):
        try:
            full_df = self.read_parquet(table_name)
            if 'id' in full_df.columns:
                return full_df['id']
            else:
                print("Column 'id' not found in parquet files")
                return pd.Series(dtype=int)
        except Exception as e:
            print(f"Failed to retrieve parquet files in get_ids: {e}")
            return pd.Series(dtype=int)