import requests
from dataclasses import dataclass, fields
from typing import List
import json
from load import DataLoader

class ApiRequest:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def extractData(self, path: str, dataclass_name: dataclass, offset: int = 0, limit: int = 20) -> List[dict]:
        data = []
        url = f"{self.endpoint}?offset={offset}&limit={limit}"
        table = DataLoader(path, "bronze")
        ids = table.get_ids(dataclass_name.__name__.lower())
        if not ids.empty: 
            ids = set(int(i) for i in ids)

        response = requests.Session().get(url).json()
        results = response.get("results")
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
            filtered_response = {}
            for key, default in dataclass_fields.items():
                value = detail_response.get(key, default)

                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                
                filtered_response[key] = value

            dataclass_results = dataclass_name(**filtered_response)
            data.append(dataclass_results)  
        # print(f"Current Progress: {item['url'].split('/')[-2]}")
        return data or None