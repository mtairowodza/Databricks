# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze Streaming Table

# COMMAND ----------

data_path = "abfss://iot@adlsledemo001.dfs.core.windows.net/bronze/"



# COMMAND ----------

@dlt.table
def  data_bronze():
    return spark.readStream.format("cloudfiles").option("cloudFiles.format", "parquet").option("header", True).option("inferSchema", True).load(data_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Silver Streaming Table

# COMMAND ----------

@dlt.table(
  comment="iot dimension in the silver layer",
  path = "abfss://iot@adlsledemo001.dfs.core.windows.net/python/silver"
)
def data_silver():
    return(
        dlt.read_stream("data_bronze")
            .select(
            'messageId',
            'deviceId',
            'temperature',
            'humidity',
            to_date('EventProcessedUtcTime', "dd-MMM-yy kk.mm.ss.SS").alias('EventTime')
                ).withColumnRenamed('messageId', 'MessageId')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Table

# COMMAND ----------

@dlt.table(
  comment="iot dimension in the gold layer",
  path = "abfss://iot@adlsledemo001.dfs.core.windows.net/python/gold/dimlatestreading" 
)
def data_gold():
    df = dlt.read("data_silver")
    dt = df.sort(df['EventTime'].desc()).limit(1)
    return dt
