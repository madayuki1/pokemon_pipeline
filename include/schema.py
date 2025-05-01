from pyspark.sql.types import StructType, FloatType, IntegerType, ArrayType, StructField, StringType
import pandas as pd
from typing import Dict
import json

def infer_schema(data:pd.DataFrame) -> StructType:

    def infer_type(value):
        if isinstance(value, int):
            return IntegerType()
        elif isinstance(value, float):
            return FloatType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, dict):
            struct_fields = [StructField(k, infer_type(v), True) for k, v in value.items()]
            return StructType(struct_fields)
        elif isinstance(value, list) and value:
            if isinstance(value[0], dict):
                struct_fields = [StructField(k, infer_type(v), True) for k, v in value[0].items()]
                return ArrayType(StructType(struct_fields))
            return ArrayType(infer_type(value[0]))
        else:
            return StringType()

    if data.empty:
        raise ValueError("Cannot infer schema from empty Dataframe")
    
    first_row = data.iloc[0].to_dict()
    fields = StructType([StructField(col, infer_type(value), True) for col, value in first_row.items()])

    return fields