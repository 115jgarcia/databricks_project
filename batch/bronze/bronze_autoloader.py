# Databricks notebook source
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
                .outputMode("append")
                .format("delta")
                .option("mergeSchema", "true")
                .option("checkpointLocation", f"{checkpoint_dir}/{name}_bronze")
                .trigger(availableNow=True)
                .table(f"bronze.{name}_bronze"))
    query.awaitTermination()

for f in files:
    print(f"Loading: {f.path}\tTable: {f.name[:-1]}_bronze")
    load_tables(path=f.path, name=f.name[:-1])
