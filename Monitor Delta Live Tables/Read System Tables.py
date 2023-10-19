# Databricks notebook source
# Specify the Delta table path
delta_table_path = "dbfs:/pipelines/8d4c228b-0e3a-46bc-b5e6-9631bd803325/system/events/"

# Read data from the Delta table using the Delta format
delta_table = spark.read.format("delta").load(delta_table_path)
display(delta_table)
