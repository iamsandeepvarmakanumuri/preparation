# source ./python_3_12_4_venv/bin/activate

import pandas as pd
import numpy as np
import pendulum
import psycopg2
print("Sandeep is tesing")

# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SimpleTest").getOrCreate()
print("Pyspark Hello Leute !!")

data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]
schema = ["String", "Int64"]

df = spark.createDataFrame(data, columns, schema)

print("Original DataFrame: ")
df.show()

# Perform a basic transformation: Filter out people younger than 30
df_filtered = df.filter(col("Age") >= 30)

# Show the transformed DataFrame
print("Filtered DataFrame (Age >= 30):")
df_filtered.show()

# Stop the Spark session
spark.stop()