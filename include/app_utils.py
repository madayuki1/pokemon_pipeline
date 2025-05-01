from typing import List
from pandas import DataFrame, to_datetime
from datetime import datetime
import os
import pandas as pd
import json

from extract import *

def create_dataframe(data: List[dict]) -> DataFrame:
    return DataFrame(data)

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