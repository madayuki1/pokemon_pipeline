import requests
import data_classes as dc
from dataclasses import dataclass, fields, MISSING
from typing import List
import json
from transform import *
from app_utils import *
import pandas as pd
import pprint
import load

BASE_URL = "https://pokeapi.co/api/v2"

POKEMON_ENDPOINTS = "pokemon"
TYPE_ENDPOINTS = "type"
ABILITY_ENDPOINTS = "ability"


class ApiRequest:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def extractData(self, dataclass_name: dataclass, offset: int = 0, limit: int = 20) -> List[dict]:
        data = []
        url = f"{self.endpoint}?offset={offset}&limit={limit}"
        table = DataLoader("bronze")
        ids = table.get_ids(path=TABLES[f"{dataclass_name.__name__.lower()}"])
        ids = set(int(i) for i in ids)
        # print(f'existing ids: {ids}')
        print(f'url use in ability {url}')

        response = requests.Session().get(url).json()
        results = response.get("results")
        # print(f'total count {len(results)}')
        if not results:
            return None
        
        url = response.get("next")
        for item in results:
            id = int(item["url"].split("/")[-2])

            if id in ids:
                continue

            detail_response = requests.Session().get(f"{self.endpoint}/{id}").json()

            # get the fields defined in the dataclass
            dataclass_fields = {
                field.name: field.default for field in fields(dataclass_name)
            }

            # filter detail_response to only provided key:value of the dataclass field
            filtered_response = {
                key: detail_response.get(key, default)
                for key, default in dataclass_fields.items()
            }

            dataclass_results = dataclass_name(**filtered_response)
            data.append(dataclass_results)  
        # print(f"Current Progress: {item['url'].split('/')[-2]}")
        return data or None


if __name__ == "__main__":
    try:
        data = ApiRequest(f"{BASE_URL}/{POKEMON_ENDPOINTS}").extractData(
            dataclass_name=dc.Pokemon
        )
        df = pd.DataFrame(data)
        df.to_csv()
        # pass
    except Exception as e:
        print(f"Error : {e}")
