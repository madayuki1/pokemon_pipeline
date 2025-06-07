from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta 
import sys
import requests
from pathlib import Path
import yaml
import duckdb
from jinja2 import Template

sys.path.append(str((Path(__file__).parent.parent / "include").resolve()))

from extract import  ApiRequest
from load import DataLoader
from app_utils import get_config, get_class

dag_owner = 'madayuki'
current_path = Path(__file__).resolve()
root_path = current_path.parents[3]
data_path = root_path / 'data' / 'poke_api'
sql_path = current_path.parents[1] / 'include' / 'sql'

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

    def extract_and_load_bronze_data(table_name, endpoint, dataclass_instance, call_mode, batch_size):
        bronze_loader = DataLoader(data_path, 'bronze')
        
        def single_call(endpoint, dataclass_instance):
            data = ApiRequest(endpoint).extractData(data_path, table_name, dataclass_instance)
            transformation_class = get_class(f'{dataclass_instance.__name__}Transformation', 'transform')
            transformed_data = transformation_class(data).bronze()
            bronze_loader.save_parquet(transformed_data, table_name)
        
        def batch_call(endpoint, dataclass_instance, batch_size):
            offset = 0

            print(f"Starting batch call for {endpoint}.")
            while True:
                batch_data = ApiRequest(endpoint).extractData(data_path, table_name, dataclass_instance, offset=offset, limit=batch_size)

                if not batch_data:
                    print("No more data found. Ending batch.")
                    break

                transformation_class = get_class(f'{dataclass_instance.__name__}Transformation', 'transform')
                transformed_batch = transformation_class(batch_data).bronze()
                bronze_loader.save_parquet(transformed_batch, table_name)

                if len(batch_data) < batch_size:
                    print("Last batch received. Ending batch.")
                    break

                offset = offset + batch_size
            print(f"Finished batch call for {endpoint}.")
        
        @task(task_id=f'Extract_{dataclass_instance.__name__}')
        def extract_data():
            print(f'this le path{data_path}')
            if call_mode == 'single':
                single_call(endpoint, dataclass_instance)
            elif call_mode == 'batch':
                batch_call(endpoint, dataclass_instance, batch_size)

        return extract_data()

    def transform_and_load_silver_data(mode:str, table_name:str, config_dict: dict):
        bronze_loader = DataLoader(data_path, 'bronze')
        silver_loader = DataLoader(data_path, 'silver')
        con = duckdb.connect(data_path / 'silver.duckdb')

        @task(task_id = f'Transform_{table_name}')
        def transform_data():
            if mode == 'process_bronze':
                bronze_data = bronze_loader.read_parquet(table_name)
                transformation_class = get_class(f'{config_dict.get('class')}Transformation', 'transform')
                if config_dict.get('return_multiple'):
                    for name, df in transformation_class(bronze_data).silver():
                        silver_loader.save_parquet(df, name, filename=name)
                else:
                    silver_data = transformation_class(bronze_data).silver()    
                    silver_loader.save_parquet(silver_data, table_name, filename=table_name)
            else:
                # Read the SQL file content as string
                sql_str = (sql_path / f'{table_name}.sql').read_text() 

                jinja_params = {
                    'bronze_path' : str(data_path / 'bronze'),
                    'silver_path' : str(data_path / 'silver'),
                    'gold_path' : str(data_path / 'gold')
                }
                query = Template(sql_str).render(**jinja_params)
                silver_data = con.execute(query).fetchdf()
                silver_loader.save_parquet(silver_data, table_name, filename=table_name)


        return transform_data()

    def transform_and_load_gold_data(table_name):
        gold_loader = DataLoader(data_path, 'gold')
        con = duckdb.connect(data_path / 'gold.duckdb')

        @task(task_id=f'Transform_{table_name}')
        def transform_data():
            # Read the SQL file content as string
            sql_str = (sql_path / f'{table_name}.sql').read_text() 

            jinja_params = {
                'bronze_path' : str(data_path / 'bronze'),
                'silver_path' : str(data_path / 'silver'),
                'gold_path' : str(data_path / 'gold')
            }

            query = Template(sql_str).render(**jinja_params)
            gold_data = con.execute(query).fetchdf()
            gold_loader.save_parquet(gold_data, table_name, filename=table_name)
        return transform_data()

    def generate_visualization(viz_name: str):
        @task(task_id = f'visualize_{viz_name}')
        def visualize():
            parquet_file = data_path / 'gold'
            output_path = root_path / 'data' / 'reports'
            viz_class = get_class(f'{viz_name}_plot', 'visualization')
            viz_class(parquet_file, output_path)

        return visualize()

    with TaskGroup("Bronze_Layer") as bronze_group:
        
        bronze_config = yaml.safe_load(
            get_config('bronze_etl_plan.yml').read_text())['bronze_etl_plan']
        tasks = {}

        for name, params in bronze_config.items():
            tasks[name] = extract_and_load_bronze_data(
                table_name = name,
                endpoint=params['endpoint'],
                dataclass_instance=get_class(params['class'], 'data_classes'),
                call_mode=params['call_mode'],
                batch_size=params['batch_size']
            )

        for name, params in bronze_config.items():
            for dependencies in params.get('dependencies', []):
                if dependencies not in tasks:
                    continue
                tasks[dependencies] >> tasks[name]

    with TaskGroup('Silver_Layer') as silver_group:
        silver_config = yaml.safe_load(
            get_config('silver_etl_plan.yml').read_text())
        for mode, config_group in silver_config.items():
            for name, params in config_group.items():
                tasks[name] = transform_and_load_silver_data(
                    mode = mode,
                    table_name=name,
                    config_dict = params
                )
                for dependencies in params.get('dependencies', []):
                    if dependencies not in tasks:
                        continue
                    tasks[dependencies] >> tasks[name]

    with TaskGroup('Gold_Layer') as gold_group:
        gold_config = yaml.safe_load(
            get_config('gold_etl_plan.yml').read_text())['gold_etl_plan']
        for name, params in gold_config.items():
            tasks[name] = transform_and_load_gold_data(
                table_name=name
            )
            for dependencies in params.get('dependencies', []):
                if dependencies not in tasks:
                    continue
                tasks[dependencies] >> tasks[name]
    with TaskGroup('Visualization') as viz_group:
        gold_config = yaml.safe_load(
            get_config('gold_etl_plan.yml').read_text())['gold_etl_plan']
        for name, params in gold_config.items():
            tasks[name] = generate_visualization(
                viz_name=name
            )
            for dependencies in params.get('dependencies', []):
                if dependencies not in tasks:
                    continue
                tasks[dependencies] >> tasks[name]

    (
        check_api_response()
        >> bronze_group
        >> silver_group
        >> gold_group
        >> viz_group
    )

pokeapi_pipeline()
