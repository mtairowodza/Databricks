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

# MAGIC %md
# MAGIC # Manage data with Delta Lake
# MAGIC -This lesson will focus primarily on the pattern used to create most tables, CREATE TABLE _ AS SELECT (CTAS) statements
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Use CTAS statements to create Delta Lake tables
# MAGIC - Create new tables from existing views or tables
# MAGIC - Enrich loaded data with additional metadata
# MAGIC - Declare table schema with generated columns and descriptive comments
# MAGIC - Set advanced options to control data location, quality enforcement, and partitioning
# MAGIC - Create shallow and deep clones

# COMMAND ----------

# MAGIC %run  "/Workspace/Repos/munyat@godevsuite313.onmicrosoft.com/Databricks/data-engineering-with-databricks/DE 3 - Delta Lake/Includes/Classroom-Setup-03.2"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales AS
# MAGIC SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;
# MAGIC
# MAGIC DESCRIBE EXTENDED sales;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
# MAGIC
# MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.
# MAGIC
# MAGIC CTAS statements also do not support specifying additional file options.
# MAGIC
# MAGIC We can see how this would present significant limitations when trying to ingest data from CSV files.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_unparsed AS
# MAGIC SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;
# MAGIC
# MAGIC SELECT * FROM sales_unparsed;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC To correctly ingest this data to a Delta Lake table, we'll need to use a reference to the files that allows us to specify options.
# MAGIC
# MAGIC In the previous lesson, we showed doing this by registering an external table. Here, we'll slightly evolve this syntax to specify the options to a temporary view, and then use this temp view as the source for a CTAS statement to successfully register the Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sales_tmp_vw
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
# MAGIC   header = "true",
# MAGIC   delimiter = "|"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE sales_delta AS
# MAGIC   SELECT * FROM sales_tmp_vw;
# MAGIC   
# MAGIC SELECT * FROM sales_delta

# COMMAND ----------

# MAGIC  %md
# MAGIC
# MAGIC  
# MAGIC ## Filtering and Renaming Columns from Existing Tables
# MAGIC
# MAGIC Simple transformations like changing column names or omitting columns from target tables can be easily accomplished during table creation.
# MAGIC
# MAGIC The following statement creates a new table containing a subset of columns from the **`sales`** table. 
# MAGIC
# MAGIC Here, we'll presume that we're intentionally leaving out information that potentially identifies the user or that provides itemized purchase details. We'll also rename our fields with the assumption that a downstream system has different naming conventions than our source data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE purchases AS
# MAGIC SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
# MAGIC FROM sales;
# MAGIC
# MAGIC SELECT * FROM purchases

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we could have accomplished this same goal with a view, as shown below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW purchases_vw AS
# MAGIC SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
# MAGIC FROM sales;
# MAGIC
# MAGIC SELECT * FROM purchases_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC ## Declare Schema with Generated Columns
# MAGIC
# MAGIC As noted previously, CTAS statements do not support schema declaration. We note above that the timestamp column appears to be some variant of a Unix timestamp, which may not be the most useful for our analysts to derive insights. This is a situation where generated columns would be beneficial.
# MAGIC
# MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table (introduced in DBR 8.3).
# MAGIC
# MAGIC The code below demonstrates creating a new table while:
# MAGIC 1. Specifying column names and types
# MAGIC 1. Adding a <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">generated column</a> to calculate the date
# MAGIC 1. Providing a descriptive column comment for the generated column

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE purchase_dates (
# MAGIC   id STRING, 
# MAGIC   transaction_timestamp STRING, 
# MAGIC   price STRING,
# MAGIC   date DATE GENERATED ALWAYS AS (
# MAGIC     cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
# MAGIC     COMMENT "generated based on `transactions_timestamp` column")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Because **`date`** is a generated column, if we write to **`purchase_dates`** without providing values for the **`date`** column, Delta Lake automatically computes them.
# MAGIC
# MAGIC **NOTE**: The cell below configures a setting to allow for generating columns when using a Delta Lake **`MERGE`** statement. We'll see more on this syntax later in the course.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true; 
# MAGIC
# MAGIC MERGE INTO purchase_dates a
# MAGIC USING purchases b
# MAGIC ON a.id = b.id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Add a Table Constraint
# MAGIC
# MAGIC The error message above refers to a **`CHECK constraint`**. Generated columns are a special implementation of check constraints.
# MAGIC
# MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
# MAGIC
# MAGIC Databricks currently support two types of constraints:
# MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
# MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
# MAGIC
# MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
# MAGIC
# MAGIC Below, we'll add a **`CHECK`** constraint to the **`date`** column of our table. Note that **`CHECK`** constraints look like standard **`WHERE`** clauses you might use to filter a dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED purchase_dates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Enrich Tables with Additional Options and Metadata
# MAGIC
# MAGIC So far we've only scratched the surface as far as the options for enriching Delta Lake tables.
# MAGIC
# MAGIC Below, we show evolving a CTAS statement to include a number of additional configurations and metadata.
# MAGIC
# MAGIC Our **`SELECT`** clause leverages two built-in Spark SQL commands useful for file ingestion:
# MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
# MAGIC * **`input_file_name()`** records the source data file for each record in the table
# MAGIC
# MAGIC We also include logic to create a new date column derived from timestamp data in the source.
# MAGIC
# MAGIC The **`CREATE TABLE`** clause contains several options:
# MAGIC * A **`COMMENT`** is added to allow for easier discovery of table contents
# MAGIC * A **`LOCATION`** is specified, which will result in an external (rather than managed) table
# MAGIC * The table is **`PARTITIONED BY`** a date column; this means that the data from each data will exist within its own directory in the target storage location
# MAGIC
# MAGIC **NOTE**: Partitioning is shown here primarily to demonstrate syntax and impact. Most Delta Lake tables (especially small-to-medium sized data) will not benefit from partitioning. Because partitioning physically separates data files, this approach can result in a small files problem and prevent file compaction and efficient data skipping. The benefits observed in Hive or HDFS do not translate to Delta Lake, and you should consult with an experienced Delta Lake architect before partitioning tables.
# MAGIC
# MAGIC **As a best practice, you should default to non-partitioned tables for most use cases when working with Delta Lake.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE users_pii
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "${da.paths.working_dir}/tmp/users_pii"
# MAGIC PARTITIONED BY (first_touch_date)
# MAGIC AS
# MAGIC   SELECT *, 
# MAGIC     cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
# MAGIC     current_timestamp() updated,
# MAGIC     input_file_name() source_file
# MAGIC   FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
# MAGIC   
# MAGIC SELECT * FROM users_pii;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC The metadata fields added to the table provide useful information to understand when records were inserted and from where. This can be especially helpful if troubleshooting problems in the source data becomes necessary.
# MAGIC
# MAGIC All of the comments and properties for a given table can be reviewed using **`DESCRIBE TABLE EXTENDED`**.
# MAGIC
# MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED users_pii

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Cloning Delta Lake Tables
# MAGIC Delta Lake has two options for efficiently copying Delta Lake tables.
# MAGIC
# MAGIC **`DEEP CLONE`** fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE purchases_clone
# MAGIC DEEP CLONE purchases

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Because all the data files must be copied over, this can take quite a while for large datasets.
# MAGIC
# MAGIC If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, **`SHALLOW CLONE`** can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE purchases_shallow_clone
# MAGIC SHALLOW CLONE purchases

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


