import sys
import os
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath("/opt/airflow/include"))

import spark_load

spark = SparkSession.builder \
    .appName("ManualTest") \
    .getOrCreate()

spark_manager = spark_load.SparkManager(spark_load.BRONZE_TYPE_TABLE, spark)
df = spark_manager.readData()
df.show()