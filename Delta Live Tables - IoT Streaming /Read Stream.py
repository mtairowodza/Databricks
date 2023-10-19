# Databricks notebook source

adls_path = "abfss://iot@adlsledemo001.dfs.core.windows.net/silver/"
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.functions import desc

# Read streaming data from Delta Lake table
df = spark.readStream.format("delta").option("header", "true").load(adls_path)

# Apply transformations
dt = df.orderBy(desc("EventTime")).limit(1)

# Display the result
display(dt)

