from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta 
from zoneinfo import ZoneInfo
import sys
import os
import requests

sys.path.append(os.path.abspath("/opt/airflow/include"))
# sys.path.append(os.path.abspath("../include"))

from extract import POKEMON_ENDPOINTS, TYPE_ENDPOINTS, ABILITY_ENDPOINTS, ApiRequest, BASE_URL
from transform import TypeTransformation, AbilityTransformation, PokemonTransformation
from load import DataLoader, TABLES
from data_classes import Pokemon, Type, Ability

dag_owner = 'madayuki'

default_args = {
        'owner': dag_owner,
        'depends_on_past': False,
        'schedule_interval': None,
        'start_date': datetime(2025, 1, 1),
        'retries': 0,
        'retry_delay': timedelta(minutes=1)
        }

@dag(
    "pokeapi_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False
)
def pokeapi_pipeline():
    @task(task_id="Check_API_Response")
    def check_api_response():
        url = 'https://pokeapi.co/api/v2'
        response = requests.Session().get(url)

        if response.status_code==200:
            print('API Reached')
        else:
            raise ValueError(f"API not reached, status code = {response.status_code}")

    def extract_and_load_bronze_data(endpoint, dataclass_instance, path):
        bronze_loader = DataLoader('bronze')
        url = f'{BASE_URL}/{endpoint}'
        
        def single_call(endpoint, dataclass_instance):
            now = datetime.now(ZoneInfo("Asia/Singapore"))
            filename = f'bronze_{now.strftime("%Y-%m-%d__%H-%M-%S")}'
            data = ApiRequest(url).extractData(dataclass_instance)
            transformed_data = TypeTransformation(data).bronze()
            bronze_loader.save_parquet(transformed_data, path, filename)
        
        def batch_call(endpoint, dataclass_instance):
            offset = 0
            batch_size = 200

            print(f"Starting batch call for {endpoint}.")
            while True:
                batch_data = ApiRequest(url).extractData(dataclass_instance, offset=offset, limit=batch_size)
                now = datetime.now(ZoneInfo("Asia/Singapore"))
                filename = f'bronze_{now.strftime("%Y-%m-%d__%H-%M-%S")}'

                if not batch_data:
                    print("No more data found. Ending batch.")
                    break

                if endpoint == POKEMON_ENDPOINTS:
                    transformed_batch = PokemonTransformation(batch_data).bronze()
                elif endpoint == ABILITY_ENDPOINTS:
                    transformed_batch = AbilityTransformation(batch_data).bronze()
                bronze_loader.save_parquet(transformed_batch, path, filename)

                if len(batch_data) < batch_size:
                    print("Last batch received. Ending batch.")
                    break

                offset = offset + batch_size
            print(f"Finished batch call for {endpoint}.")
        
        @task(task_id=f'Extract_{endpoint}')
        def extract_data():
            if endpoint == TYPE_ENDPOINTS:
                single_call(endpoint, dataclass_instance)
            elif endpoint in [ABILITY_ENDPOINTS, POKEMON_ENDPOINTS]:
                batch_call(endpoint, dataclass_instance)

        return extract_data()

    def transform_and_load_silver_data(table):
        label = table.split('/')[-1]
        bronze_loader = DataLoader('bronze')
        silver_loader = DataLoader('silver')

        @task(task_id = f'Transform_{label}')
        def transform_data():
            now = datetime.now(ZoneInfo("Asia/Singapore"))
            bronze_data = bronze_loader.read_parquet(table, 'bronze')
            if table == TABLES['type']:
                df_silver_data = TypeTransformation(bronze_data).silver()
            elif table == TABLES['ability']:
                df_silver_data = AbilityTransformation(bronze_data).silver()
            elif table == TABLES['pokemon']:
                df_silver_data = PokemonTransformation(bronze_data).silver()    
            silver_loader.save_parquet(df_silver_data, table, f'silver_{now.strftime("%Y-%m-%d__%H-%M-%S")}')

        return transform_data()

    def transform_and_load_gold_data(table):
        label = table.split('/')[-1]
        silver_loader = DataLoader('silver')
        gold_loader = DataLoader('gold')

        @task(task_id=f'Transform_{label}')
        def transform_data():
            now = datetime.now(ZoneInfo("Asia/Singapore"))
            silver_data = silver_loader.read_parquet(table, 'silver')

            if table == TABLES['type']:
                gold_data = TypeTransformation(silver_data).gold()
            elif table == TABLES['ability']:
                gold_data = AbilityTransformation(silver_data).gold()
            elif table == TABLES['pokemon']:
                gold_data = PokemonTransformation(silver_data).gold()
            gold_loader.save_parquet(gold_data, table, f'gold_{now.strftime("%Y-%m-%d__%H-%M-%S")}')
        return transform_data()

    with TaskGroup("Bronze_Layer") as bronze_group:
        bronze_type = extract_and_load_bronze_data(TYPE_ENDPOINTS, Type, TABLES['type'])
        bronze_ability = extract_and_load_bronze_data(ABILITY_ENDPOINTS, Ability, TABLES['ability'])
        bronze_pokemon = extract_and_load_bronze_data(POKEMON_ENDPOINTS, Pokemon, TABLES['pokemon'])
        bronze_type >> bronze_ability >> bronze_pokemon

    with TaskGroup('Silver_Layer') as silver_group:
        silver_type =transform_and_load_silver_data(TABLES['type'])
        silver_ability =transform_and_load_silver_data(TABLES['ability'])
        silver_pokemon =transform_and_load_silver_data(TABLES['pokemon'])
        silver_type >> silver_ability >> silver_pokemon
    
    with TaskGroup('Gold_Layer') as gold_group:
        gold_type =transform_and_load_gold_data(TABLES['type'])
        gold_ability =transform_and_load_gold_data(TABLES['ability'])
        gold_pokemon =transform_and_load_gold_data(TABLES['pokemon'])
        gold_type >> gold_ability >> gold_pokemon

    (
        check_api_response()
        >> bronze_group
        >> silver_group
        >> gold_group
    )

pokeapi_pipeline()