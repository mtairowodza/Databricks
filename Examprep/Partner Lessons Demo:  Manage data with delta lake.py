# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Manage data with delta lake
# MAGIC
# MAGIC - DDL data definition language to
# MAGIC   1. create tables
# MAGIC   1. compact files
# MAGIC   1. restore previous table versions
# MAGIC   1. add perform garbage collection of tables in the lakehouse
# MAGIC - use CREATE TABLE AS (CTAS) to store data derived from a query in a DELTA lake table
# MAGIC - use sql to perform complete and incremental updates to existing tables

# COMMAND ----------

# MAGIC %md
# MAGIC # What is Delta lake
# MAGIC
# MAGIC - An open source project that enables building a data lakehouse on top of existing cloud storage
# MAGIC - Not
# MAGIC  1. Proprietory tech
# MAGIC  1. Storage format
# MAGIC  1. Storage medium
# MAGIC  1. Database service or data warehouse
# MAGIC
# MAGIC - Is
# MAGIC  1. Open source
# MAGIC  1. Builds upon standard for cloud object storage
# MAGIC  1. Built for scalable metadata handling
# MAGIC  1. Dev by DB but mad open source
# MAGIC - stores data in Parquet as well as Json
# MAGIC - optimised for the behaviour of object based cloud storage
# MAGIC - object storage cheap, durable, scalable infinitly
# MAGIC - to solve problem of quickly returning queries
# MAGIC - decouples compute and storage costs
# MAGIC - powering apps ad quiries
# MAGIC
# MAGIC ## Brings ACID to object storage
# MAGIC - Atomicity - means all transactions either succed or fail completely
# MAGIC - Consistency - guarantees relate to how a given state of the dat is observed by simulteneous operations
# MAGIC - Isolation regers to how simultaneus operations conflict with one another. The isolation guarantees that Delta Lake provides do differ from other systems
# MAGIC - Durability - means that committed changes are permanent
# MAGIC
# MAGIC - at table level
# MAGIC
# MAGIC ## Problems solved by ACID
# MAGIC
# MAGIC - Hard to append data
# MAGIC - Modificatin of existing data difficult - update and delete
# MAGIC - Jobs failing midway- jobs will either fail or succed completely
# MAGIC - real time operations hard
# MAGIC - costly to keep historical data versions - transaction logs and snapshot queries
# MAGIC ### Delta Lake is the default format for tables created in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC # Shemas and Tables
# MAGIC
# MAGIC # Schemas and Tables on Databricks
# MAGIC In this demonstration, you will create and explore schemas and tables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC
# MAGIC * Use Spark SQL DDL to define schemas and tables
# MAGIC * Differentiate between managed and external tables in Spark SQL 
# MAGIC * Explain how managed and external tables impact storage location and management
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Resources**
# MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>
# MAGIC
# MAGIC
# MAGIC - With no location specified goes into default location
# MAGIC - with location specied goes into that location

# COMMAND ----------

# MAGIC %run  "/Workspace/Repos/munyat@godevsuite313.onmicrosoft.com/Databricks/data-engineering-with-databricks/DE 3 - Delta Lake/Includes/Classroom-Setup-03.1"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location LOCATION  '${da.paths.working_dir}/${da.schema_name}_custom_location';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED ${da.schema_name}_custom_location

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${da.schema_name}_default_location; 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
# MAGIC INSERT INTO managed_table 
# MAGIC VALUES (3, 2, 1);
# MAGIC SELECT * FROM managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_table;

# COMMAND ----------

tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
print(tbl_location)

files = dbutils.fs.ls(tbl_location)
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_table;

# COMMAND ----------

schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
print(schema_default_location)
dbutils.fs.ls(schema_default_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${da.schema_name}_default_location;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
# MAGIC   path = '${da.paths.datasets}/flights/departuredelays.csv',
# MAGIC   header = "true",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );
# MAGIC CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
# MAGIC   SELECT * FROM temp_delays;
# MAGIC
# MAGIC SELECT * FROM external_table;

# COMMAND ----------

spark.sql("DESCRIBE EXTENDED external_table").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA ${da.schema_name}_custom_location;
# MAGIC DROP SCHEMA ${da.schema_name}_default_location;

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


