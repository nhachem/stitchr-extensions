"""
pyspark

import df_transform

"""
# import typing
# import typing_extensions

# from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import DataFrame
from pyspark.sql.dataframe import DataFrame

# from random import random
# from random import choice

# from string import ascii_letters
import os
# import sys

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

test_df: DataFrame = spark.createDataFrame([
    ("A", 0, 1),
    ("B", 1, 2),
    ("C", 2, 3)],
    schema=("K E   Y", "cols with   sp  aces", " .value"))

test_df1: DataFrame = spark.createDataFrame([
    ("A", 0, 1),
    ("B", 1, 2),
    ("C", 2, 3)],
    schema=("K E   Y", "cols with   sp  aces", "value"))


simpleData = (("James", "", "Smith", 36636, "NewYork", 3100),
              ("Michael", "", "Rose", 40288, "California", 4300),
              ("Robert", "", "Williams", 42114, "Florida", 1400),
              ("Maria", "Anne", "Jones", 39192, "Florida", 5500),
              ("Jen", "Mary", "Brown", 34561, "NewYork", 3000)
              )
columns = ["firstname", "middlename", "lastname", "id", "location", "salary"]

simple_df = spark.createDataFrame(data=simpleData, schema=columns)

print(os.getcwd())
# path = os.getcwd()
# os.chdir(f'../resources')
python_root_path = f"{os.environ.get('ROOT_DIR')}/python"
"""
# this is a sample from UK companies data persons-with-significant-control snapshot 2021-01-11
df_uk_companies_holdings: DataFrame = spark\
    .read\
    .format("json")\
    .load(f"{python_root_path}/resources/json_sample_data.json")
"""

# need to make a deterministic json with arrays maybe 2 arrays
df_json: DataFrame = spark\
    .read\
    .format("json")\
    .option("multiline", "true")\
    .load(f"{python_root_path}/resources/json_test.json")
