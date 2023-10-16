# Databricks notebook source
# MAGIC %md
# MAGIC #Connection strings to ADLS gen 2 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsledemo001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsledemo001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsledemo001.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-01T03:42:44Z&st=2023-10-12T19:42:44Z&spr=https&sig=p6fWGyMnwKMNTDMBRx5J%2B8cTEYkqNujm00xD%2FZafLZk%3D")
