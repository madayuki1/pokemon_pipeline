from typing import List, Union
import pandas as pd
import inspect
import json

from app_utils import create_dataframe, add_timestamp, validate_columns, get_short_effect_en

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
    
class MoveTransformation:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def bronze(self):
        df = create_dataframe(self.data)
        return df

    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = ["generation"]
            validate_columns(
                df, 
                col_to_drop, 
                context='drop',
                caller_frame=inspect.currentframe())

            df = df.drop(columns=["generation"])
        except ValueError as e:
            print(e)
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "move_id"}
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
        try:
            col_to_drop = ["generation", "names", "pokemon", "effect_changes"]
            validate_columns(
                df, 
                col_to_drop, 
                context='drop',
                caller_frame=inspect.currentframe())

            df['short_effect_en'] = df['effect_entries'].apply(get_short_effect_en)
            df = df.drop(columns=["generation", "names", "pokemon", "effect_changes", "effect_entries"])

        except ValueError as e:
            print(e)
            
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
        pokemon_df = add_timestamp(self.data)
        try:
            col_to_drop = ["cries", "game_indices"]
            validate_columns(
                pokemon_df, 
                col_to_drop, 
                context='drop',
                caller_frame=inspect.currentframe())

            pokemon_df = pokemon_df.drop(columns=["cries", "game_indices"])
        except ValueError as e:
            print(e)
        pokemon_moves = []
        pokemon_abilities = []
        pokemon_types = []

        for _, row in pokemon_df.iterrows():
            pokemon_id = row['id']
            types = json.loads(row['types']) if isinstance(row['types'], str) else row['types']
            moves = json.loads(row['moves']) if isinstance(row['moves'], str) else row['moves']
            abilities = json.loads(row['abilities'] if isinstance(row['abilities'], str) else row['abilities'])

            for type in types:
                type_url = type['type']['url']
                type_id = type_url.split('/')[-2]
                pokemon_types.append({
                    'pokemon_id': pokemon_id,
                    'type_id': type_id
                })

            for move in moves:
                move_url = move['move']['url']
                move_id = move_url.split('/')[-2]
                pokemon_moves.append({
                    'pokemon_id': pokemon_id,
                    'move_id': move_id
                })

            for ability in abilities:
                ability_url = ability['ability']['url']
                ability_id = ability_url.split('/')[-2]
                pokemon_abilities.append({
                    'pokemon_id': pokemon_id,
                    'ability_id': ability_id
                })
        pokemon_df = pokemon_df.drop(columns=['types', 'moves', 'abilities'])

        pokemon_types_df = pd.DataFrame(pokemon_types)
        pokemon_moves_df = pd.DataFrame(pokemon_moves)
        pokemon_abilities_df = pd.DataFrame(pokemon_abilities)
        yield 'pokemon', pokemon_df
        yield 'pokemon_types', pokemon_types_df
        yield 'pokemon_moves', pokemon_moves_df
        yield 'pokemon_abilities', pokemon_abilities_df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "pokemon_id"}
        )
        return df
