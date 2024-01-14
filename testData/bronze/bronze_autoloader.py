# Databricks notebook source
# MAGIC %run ../data_source_system

# COMMAND ----------

import pyspark.sql.functions as F

# checkpoint directory
checkpoint_dir = "gs://bankdatajg/checkpoint"

# raw paths
files = dbutils.fs.ls('gs://bankdatajg/raw')

def load_tables(path, name):
    query = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/{name}_bronze_schema")
                .option("delimiter", "|")
                .option("header", True)
                .load(path))

    query = (query
                .withColumn("filename", F.input_file_name())
                .withColumn("process_date", F.current_timestamp()))
    
    query = (query.writeStream
                .format("delta")
                .option("mergeSchema", "true")
                .option("checkpointLocation", f"{checkpoint_dir}/{name}_bronze")
                .trigger(availableNow=True)
                .table(f"bronze.{name}_bronze"))
    query.awaitTermination()

for f in files:
    print(f"Loading: {f.path}")
    load_tables(f.path, f.name[:-1])

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronze.accounts_bronze
# MAGIC SET savings_id = NULL
# MAGIC WHERE
# MAGIC 	savings_id NOT IN (
# MAGIC 		SELECT savings_id
# MAGIC 		FROM bronze.savings_bronze);
# MAGIC
# MAGIC UPDATE bronze.accounts_bronze
# MAGIC SET checkings_id = NULL
# MAGIC WHERE
# MAGIC 	checkings_id NOT IN (
# MAGIC 		SELECT checkings_id
# MAGIC 		FROM bronze.checkings_bronze)
