-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Read bronze layer 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Patients
-- MAGIC

-- COMMAND ----------

CREATE STREAMING LIVE TABLE patient_bronze 
COMMENT "raw pateints data from ADLS" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://sqlserver@adlsledemo001.dfs.core.windows.net/bronze/Patient/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Medical Stuff

-- COMMAND ----------

CREATE STREAMING LIVE TABLE medical_staff_bronze 
COMMENT "raw medical data from ADLS" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://sqlserver@adlsledemo001.dfs.core.windows.net/bronze/MedicalStaff/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Hospitals

-- COMMAND ----------

CREATE STREAMING LIVE TABLE hospital_bronze 
COMMENT "raw hospital data from ADLS" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://sqlserver@adlsledemo001.dfs.core.windows.net/bronze/Hospital/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Billing

-- COMMAND ----------

CREATE STREAMING LIVE TABLE billing_bronze 
COMMENT "raw billing data from ADLS" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://sqlserver@adlsledemo001.dfs.core.windows.net/bronze/Billing/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Dates

-- COMMAND ----------

CREATE STREAMING LIVE TABLE date_bronze 
COMMENT "raw dates data from ADLS" 
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze") 
AS
SELECT
  *
FROM
  cloud_files("abfss://sqlserver@adlsledemo001.dfs.core.windows.net/bronze/Date/","parquet");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Transformation and Write to Silver Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Patient (Slowly Changing Dimension (SCD) Type 2)
-- MAGIC

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE TABLE patient_silver_clean
(
  CONSTRAINT Valid_PatientID EXPECT (PatientID  IS NOT NULL) ON VIOLATION DROP ROW

)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze patient dataset with enforced data quality"
PARTITIONED BY (Year, Month)
AS 
SELECT 
    PatientID,
    FirstName,
    LastName,
    DateOfBirth,
    Gender,
    Address,
    Email,
    CreatedAt,
    YEAR(CreatedAt) AS   Year,
    MONTH(CreatedAt) AS  Month
FROM STREAM(LIVE.patient_bronze)

-- COMMAND ----------

--Write the cleaned dataset as SCD TYPE 2
CREATE STREAMING LIVE TABLE patient_silver
TBLPROPERTIES (delta.enableChangeDataFeed = true) --Track row level changes.
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/silver/Patient/" ;

APPLY CHANGES INTO LIVE.patient_silver
FROM STREAM(LIVE.patient_silver_clean)
  KEYS (PatientID)
  SEQUENCE BY CreatedAt
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Medical Staff

-- COMMAND ----------

CREATE  STREAMING LIVE TABLE medical_staff_silver
(
  CONSTRAINT Valid_MedicalStaffID EXPECT (StaffID IS NOT NULL) ON VIOLATION DROP ROW

)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze medical staff dataset with enforced data quality"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/silver/MedicalStaff/"
AS 
SELECT 
    StaffID,
    FirstName,
    LastName,
    Gender,
    Specialization,
    LicenseNumber,
    CreatedAt
FROM STREAM(LIVE.medical_staff_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Hospitals

-- COMMAND ----------

CREATE  STREAMING LIVE TABLE hospital_silver
(
  CONSTRAINT Valid_HospitalID EXPECT (HospitalID IS NOT NULL) ON VIOLATION DROP ROW

)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze medical staff dataset with enforced data quality"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/silver/Hospital/"
AS 
SELECT 
    HospitalID,
    HospitalName,
    Address,
    PhoneNumber,
    CreatedAt
FROM STREAM(LIVE.hospital_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Billing

-- COMMAND ----------

CREATE  STREAMING LIVE TABLE billing_silver
(
  CONSTRAINT Valid_MedicalStaffID EXPECT (StaffID IS NOT NULL) ON VIOLATION DROP ROW

)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze medical staff dataset with enforced data quality"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/silver/Billing/"
PARTITIONED BY (Year, Month)
AS 
SELECT 
    BillingID,
    PatientID,
    StaffID,
    HospitalID,
    BillingDate,
    Amount  AS BillingAmount,
    CreatedAt,
    YEAR(CreatedAt) AS   Year,
    MONTH(CreatedAt) AS  Month
FROM STREAM(LIVE.billing_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Write to Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DimPatient

-- COMMAND ----------

CREATE  LIVE TABLE DimPatients
TBLPROPERTIES ("quality" = "gold")
COMMENT "patient dimension in the gold layer"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/gold/DimPatient/"
PARTITIONED BY (Year, Month)
AS
SELECT 
*
FROM LIVE.patient_silver 
WHERE __END_AT IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DimMedicalStaff

-- COMMAND ----------

CREATE  LIVE TABLE DimMedicalStaff
TBLPROPERTIES ("quality" = "gold")
COMMENT "medical staff dimension in the gold layer"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/gold/DimMedicalStaff/"
AS
SELECT 
*
FROM LIVE.medical_staff_silver 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DimHospitals

-- COMMAND ----------

CREATE  LIVE TABLE DimHospitals
TBLPROPERTIES ("quality" = "gold")
COMMENT "hospitals dimension in the gold layer"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/gold/DimHospitals/"
AS
SELECT 
*
FROM LIVE.hospital_silver 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DimDate

-- COMMAND ----------

CREATE  LIVE TABLE DimDate
TBLPROPERTIES ("quality" = "gold")
COMMENT "date dimension in the gold layer"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/gold/DimDate/"
AS
SELECT 
*
FROM LIVE.date_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##FactBilling

-- COMMAND ----------

CREATE  LIVE TABLE FactBilling
TBLPROPERTIES ("quality" = "gold")
COMMENT "Billing fact in the gold layer"
LOCATION "abfss://sqlserver@adlsledemo001.dfs.core.windows.net/gold/FactBilling/"
PARTITIONED BY (Year, Month)
AS
SELECT 
*
FROM LIVE.billing_silver 
