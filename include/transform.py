from typing import List, Union
import pandas as pd
import inspect

from app_utils import create_dataframe, add_timestamp, validate_columns

class TypeTransformation:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def bronze(self):
        df = create_dataframe(self.data)
        return df

    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = ["generation", "game_indices", "names", "moves", "pokemon"]
            validate_columns(
                df, 
                col_to_drop, 
                context='drop',
                caller_frame=inspect.currentframe())

            df = df.drop(columns=["generation", "game_indices", "names", "moves", "pokemon"])
        except ValueError as e:
            print(e)
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
        return df

    def silver(self):
        df = add_timestamp(self.data)
        df = df.drop(columns=["generation", "names", "pokemon", "effect_changes"])
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
        return df

    def silver(self):
        df = add_timestamp(self.data)
        df = df.drop(columns=["cries", "game_indices"])
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "pokemon_id"}
        )
        return df
