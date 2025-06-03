from typing import List, Union
from pandas import DataFrame
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Type
import importlib
import json

def create_dataframe(data: Union[List[dict], DataFrame]) -> DataFrame:
    df = DataFrame(data)
    if df.empty:
        return DataFrame()
    return df

def to_datetime(date_string: str) -> str:
    date_format = [
        "%Y-%m-%d %H:%M:%S",  # Example: 2023-01-10 12:30:45
        "%Y-%m-%d"            # Example: 2023-01-10"
    ]

    for format in date_format:
        try:
            parsed_date = datetime.strptime(date_string, format)
            return parsed_date.isoformat()
        except ValueError:
            continue

def save_list_to_csv(data: list, filename="output.csv"):
    """
    Saves a list of JSON objects (dicts) to a CSV file.

    Parameters:
    - data (list): A list of dictionaries containing JSON data.
    - filename (str): The name of the CSV file to save.

    Example:
        response = [{"id": 1, "name": "Bulbasaur"}, {"id": 2, "name": "Charmander"}]
        save_list_to_csv(response, "pokemon.csv")
    """
    if not isinstance(data, list):
        raise ValueError("Input must be a list of dictionaries")

    try:
        df = pd.DataFrame(data)  # Convert list to DataFrame
        df.to_csv(filename, index=False)  # Save as CSV
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving list to CSV: {e}")

def add_timestamp(data: pd.DataFrame)-> pd.DataFrame:
    current_time = datetime.now(ZoneInfo("Asia/Singapore"))
    df = (
        data.assign(created_at=current_time)
        .assign(updated_at=current_time)
    )
    return df     

def get_short_effect_en(effect_entries_string:str):
    try:
        entries = json.loads(effect_entries_string)
        for entry in entries:
            if entry.get('language', {}).get('name') == 'en':
                return entry.get('short_effect')
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_dict_key(json_string:str, key_name:str):
    try:
        data_dict = json.loads(json_string)
        dict_key = data_dict.get(key_name)
        if not dict_key:
            return
        if key_name == 'url':
            return dict_key.split('/')[-2]
        else:
            return dict_key
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_generation(generation_string:str):
    try:
        generation = json.loads(generation_string)
        url_string = generation.get('url')
        if not url_string:
            return
        id = generation.get('url').split('/')[-2]
        return id
    
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_type(type_string:str):
    try:
        type = json.loads(type_string)
        name_string = type.get('name')
        if not name_string:
            return
        name = type.get('name')
        return name
    
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_species(species_string:str):
    try:
        species = json.loads(species_string)
        url_string = species.get('url')
        if not url_string:
            return
        id = species.get('url').split('/')[-2]
        return id
    
    except (json.JSONDecodeError, TypeError):
        return None

def validate_columns(
    data: DataFrame, 
    column_to_check: List[str], 
    context: str = "use",
    caller_frame=None)-> None:
    
    missing = [col for col in column_to_check if col not in data.columns]
    if not missing:
        return

    if caller_frame:
        class_name = caller_frame.f_locals.get('self').__class__.__name__
        method_name = caller_frame.f_code.co_name
        error_prefix = f"[{class_name}.{method_name}]"
    else:
        error_prefix = "[Validation]"
    
    action = 'Required' if context == 'use' else 'Droppable'
    raise ValueError(f'{error_prefix}; {action} columns missing: {missing}')

def get_config(config_name: str) -> Path:
    return  Path(__file__).parent.parent / "config" / f"{config_name}"

def get_class(classname: str, module: str) -> Type:
    module_name = importlib.import_module(module)
    try:
        return getattr(module_name, classname)
    except AttributeError:
        raise ValueError(f'Class {classname} not found in module {module_name}')