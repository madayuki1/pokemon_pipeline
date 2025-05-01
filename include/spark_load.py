from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType
import pandas as pd
import os
# import pyspark

BASE_PATH = "/shared/poke_api"

BRONZE_POKEMON_TABLE = F'{BASE_PATH}/bronze/pokemon'
BRONZE_TYPE_TABLE = F'{BASE_PATH}/bronze/type'
BRONZE_ABILITY_TABLE = F'{BASE_PATH}/bronze/ability'
SILVER_POKEMON_TABLE = F'{BASE_PATH}/silver/pokemon'
SILVER_TYPE_TABLE = F'{BASE_PATH}/silver/type'
SILVER_ABILITY_TABLE = F'{BASE_PATH}/silver/ability'
GOLD_POKEMON_TABLE = F'{BASE_PATH}/gold/pokemon'
GOLD_TYPE_TABLE = F'{BASE_PATH}/gold/type'
GOLD_ABILITY_TABLE = F'{BASE_PATH}/gold/ability'

TABLES = {
    "bronze_pokemon": BRONZE_POKEMON_TABLE,
    "bronze_type": BRONZE_TYPE_TABLE,
    "bronze_ability": BRONZE_ABILITY_TABLE,
    "silver_pokemon": SILVER_POKEMON_TABLE,
    "silver_type": SILVER_TYPE_TABLE,
    "silver_ability": SILVER_ABILITY_TABLE,
    "gold_pokemon": GOLD_POKEMON_TABLE,
    "gold_type": GOLD_TYPE_TABLE,
    "gold_ability": GOLD_ABILITY_TABLE
}

class SparkManager:
    def __init__(self, path, spark:SparkSession):
        self.path = path
        self.spark = spark
        self.sc = spark.sparkContext  # Use spark.sparkContext directly

        if self.sc._jsc is None:
            raise RuntimeError("SparkContext has been stopped unexpectedly.")

    def loadData(self, df:pd.DataFrame, schema: StructType):
        # Ensure the directory exists
        # os.makedirs(os.path.dirname(self.path), exist_ok=True)

        print(schema)

        data = df.to_dict(orient='records')
        spark_df = self.spark.createDataFrame(data, schema=schema)
        spark_df.repartition(4)
        spark_df.write.mode('overwrite').parquet(self.path)
        # print(f'spark_df {spark_df.show()}')

        print("Data successfully written to:", self.path)

        # # Keep session alive to inspect Spark UI
        # input("Spark job running... Press Enter to exit.")

    def readData(self):
        spark_df = self.spark.read.parquet(self.path)
        if spark_df:
            return spark_df
        return f'No Data Found in {self.path}'