# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsledemo001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsledemo001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsledemo001.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-10-16T21:11:56Z&st=2023-10-16T13:11:56Z&spr=https&sig=gaLSiX2sSo0BcKl98MLMVelfZa53CqsS9TCkuSOdWEI%3D")

# COMMAND ----------



# COMMAND ----------


