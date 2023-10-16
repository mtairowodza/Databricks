-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Read Bronze Data

-- COMMAND ----------

CREATE STREAMING LIVE TABLE iot_bronze 
COMMENT "raw data coming from IoT device" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://iot@adlsledemo001.dfs.core.windows.net/bronze/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Transformation and Write to Silver Layer

-- COMMAND ----------

CREATE STREAMING LIVE TABLE iot_silver
(
  CONSTRAINT valid_MessageID EXPECT (messageId IS NOT NULL) ON VIOLATION DROP ROW

)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze iot data with enforced data quality"
LOCATION "abfss://iot@adlsledemo001.dfs.core.windows.net/silver/"
AS SELECT 
    messageId             AS MessageId,
    deviceId              AS DevicedId,
    temperature           AS Temperature,
    humidity              AS Humidity,
    EventProcessedUtcTime AS EventTime
FROM STREAM(LIVE.iot_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Write to Gold Layer

-- COMMAND ----------

CREATE  LIVE TABLE dim_iot_latest_readings
TBLPROPERTIES ("quality" = "gold")
COMMENT "iot dimension in the gold layer"
LOCATION "abfss://iot@adlsledemo001.dfs.core.windows.net/gold/dimlatestreading/"
AS
SELECT 
*
FROM LIVE.iot_silver
ORDER BY EventTime DESC  --Latest readings
LIMIT 1

-- COMMAND ----------

--CREATE  LIVE TABLE dim_iot_last20_readings
--TBLPROPERTIES ("quality" = "gold")
--COMMENT "iot dimension in the gold layer"
--LOCATION "abfss://abfss://iot@adlsledemo001.dfs.core.windows.net/gold/DimLastTwenty/"
--AS
--SELECT 
--*
--FROM LIVE.iot_silver
--ORDER BY EventTime DESC  --Latest readings
--LIMIT 20
