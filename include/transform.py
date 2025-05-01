from typing import List, Union
import pytz
import pandas as pd
from datetime import datetime
import logging
import numpy as np
from zoneinfo import ZoneInfo

from app_utils import *
from load import *


def replace_nested_nulls(data):
    if isinstance(data, dict):
        # Recurse through dictionary keys
        # return {k: replace_nested_nulls(v) for k, v in data.items()}
        return {} if not data else {k: replace_nested_nulls(v) for k, v in data.items()}
    elif isinstance(data, list):
        # Recurse through list items
        # return [replace_nested_nulls(item) for item in data]
        return [] if not data else [replace_nested_nulls(item) for item in data]
    # elif data is None or (isinstance(data, float) and np.isnan(data)):
    elif pd.isna(data):
        # Replace None with "-"
        return "-"
    else:
        return data


def find_null_fields(df: pd.DataFrame):
    """
    Identify columns and rows with null values in a DataFrame.
    """
    # Check for null values in each column
    null_columns = df.columns[df.isnull().any()].tolist()
    if null_columns:
        print(f"Columns with null values: {null_columns}")
    else:
        print("No columns contain null values.")

    # Check for null values in each row
    null_rows = df[df.isnull().any(axis=1)]
    if not null_rows.empty:
        print(f"Rows with null values:\n{null_rows}")
    else:
        print("No rows contain null values.")


class TypeTransformation:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def bronze(self):
        df = create_dataframe(self.data)
        if df.empty:
            return pd.DataFrame()
        
        # print(df)
        default_rules = {
            "generation": lambda x: {} if x is None else x,
            # 'move_damage_class': lambda x: [] if x is None else x,
            "move_damage_class": lambda x: {} if x is None else x,
            "damage_relations": lambda x: {} if x is None else x,
            "game_indices": lambda x: [] if x is None else x,
            "moves": lambda x: [] if x is None else x,
            "names": lambda x: [] if x is None else x,
            "pokemon": lambda x: [] if x is None else x,
            "sprites": lambda x: {} if x is None else x,
        }

        for col, rule in default_rules.items():
            df[col] = df[col].apply(rule)
            df[col] = df[col].apply(replace_nested_nulls)

        return df

    def silver(self):
        current_time = datetime.now(ZoneInfo("Asia/Singapore"))
        print(f"Current time {current_time}")
        df = (
            self.data.assign(created_at=current_time)
            .assign(updated_at=current_time)
            .drop(columns=["generation", "game_indices", "names", "moves", "pokemon"])
        )
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "type_id"}
        )
        return df


class AbilityTransformation:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def bronze(self):
        df = create_dataframe(self.data)
        if df.empty:
            return pd.DataFrame()
        
        default_rules = {
            "generation": lambda x: {} if x is None else x,
            "names": lambda x: [] if x is None else x,
            "pokemon": lambda x: [] if x is None else x,
            "effect_entries": lambda x: [] if x is None else x,
            "effect_changes": lambda x: [] if x is None else x,
            "flavor_text_entries": lambda x: [] if x is None else x,
        }
        for col, rule in default_rules.items():
            df[col] = df[col].apply(rule)
            df[col] = df[col].apply(replace_nested_nulls)
        return df

    def silver(self):
        current_time = datetime.now(ZoneInfo("Asia/Singapore"))
        print(f"Current time {current_time}")
        df = (
            self.data.assign(created_at=current_time)
            .assign(updated_at=current_time)
            .drop(columns=["generation", "names", "pokemon", "effect_changes"])
        )
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "ability_id"}
        )
        return df


class PokemonTransformation:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def bronze(self):
        df = create_dataframe(self.data)
        if df.empty:
            return pd.DataFrame()
        default_rule = {
            "cries": lambda x: {} if x is None else x,
            "forms": lambda x: [] if x is None else x,
            "abilities": lambda x: [] if x is None else x,
            "game_indices": lambda x: [] if x is None else x,
            "held_items": lambda x: [] if x is None else x,
            "moves": lambda x: [] if x is None else x,
            "species": lambda x: {} if x is None else x,
            "stats": lambda x: [] if x is None else x,
            "types": lambda x: [] if x is None else x,
            "sprites": lambda x: {} if x is None else x,
        }
        for col, rule in default_rule.items():
            df[col] = df[col].apply(rule)
            df[col] = df[col].apply(replace_nested_nulls)
        # find_null_fields(df)
        df.to_csv("/shared/pokemon.csv")
        return df

    def silver(self):
        current_time = datetime.now(ZoneInfo("Asia/Singapore"))
        print(f"Current time {current_time}")
        df = (
            self.data.assign(created_at=current_time)
            .assign(updated_at=current_time)
            .drop(columns=["cries", "game_indices"])
        )
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "pokemon_id"}
        )
        return df
