# Databricks notebook source
# MAGIC %sql
# MAGIC DROP SCHEMA bronze CASCADE;
# MAGIC DROP SCHEMA silver CASCADE;

# COMMAND ----------

directories = dbutils.fs.ls('gs://bankdatajg')
for f in directories:
    dbutils.fs.rm(f.path, True)

# COMMAND ----------


