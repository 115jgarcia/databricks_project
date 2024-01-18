# Databricks notebook source
print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %run ./data_source_system

# COMMAND ----------

# MAGIC %run ./data_extraction

# COMMAND ----------

# MAGIC %md
# MAGIC Create tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA silver;
# MAGIC CREATE SCHEMA bronze;
# MAGIC CREATE SCHEMA gold;

# COMMAND ----------

# MAGIC %run ./createTables/create_bronze_tables

# COMMAND ----------

# MAGIC %run ./createTables/create_silver_tables

# COMMAND ----------

# MAGIC %run ./createTables/create_gold_tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Source System
# MAGIC Land new data

# COMMAND ----------

data_generator = generate_data()

# COMMAND ----------

# create dummy data
dataset = data_generator.create_all_tables()
data_generator.write_data(dataset=dataset)

# move dummy data
mv_data = extract()
mv_data.land_files_to_raw()

# COMMAND ----------

# checkpoint directory
checkpoint_dir = "gs://bankdatajg/checkpoint"

# raw paths
files = dbutils.fs.ls('gs://bankdatajg/raw')

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Processes
# MAGIC Create batch append only bronze tables w/ autoloader.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver processes

# COMMAND ----------

# removing dropDups fixes issue.
# how to handle files w/ dup records inserting to bronze?
# Ideal:    when new records come, only bring the newest one to update
#           when new duplicates records come -> ???

import pyspark.sql.functions as F

accounts_df = (spark.readStream
                .table("bronze.accounts_bronze")
                .select(
                    F.col("account_id").cast("int"),
                    F.col("checkings_id").cast("int"),
                    F.col("savings_id").cast("int"),
                    F.col("currency").cast("string"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"))
                )

checkings_df = (spark.readStream
                .table("bronze.checkings_bronze")
                .select(
                    F.col("checkings_id").cast("int"),
                    F.col("balance").cast("double"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("interest_rate").cast("double"),
                    F.col("monthly_fee").cast("double"),
                    F.col("routing_number").cast("string"),
                    F.col("account_number").cast("string"),
                    F.col("overdraft_protection").cast("string"),
                    F.col("is_active").cast("string"))
                )

savings_df = (spark.readStream
                .table("bronze.savings_bronze")
                .select(
                    F.col("savings_id").cast("int"),
                    F.col("balance").cast("double"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("interest_rate").cast("double"),
                    F.col("deposit_limit").cast("double"),
                    F.col("routing_number").cast("string"),
                    F.col("account_number").cast("string"),
                    F.col("overdraft_protection").cast("string"),
                    F.col("is_active").cast("string"))
                )

addresses_df = (spark.readStream
                    .table("bronze.addresses_bronze")
                    .select(
                        F.col("address_id").cast("int"),
                        F.col("address_line").cast("string"),
                        F.col("city").cast("string"),
                        F.col("state").cast("string"),
                        F.col("zipcode").cast("int"))
                    )

customers_df = (spark.readStream
                    .table("bronze.customers_bronze")
                    .select(
                        F.col("customer_id").cast("int"),
                        F.col("address_id").cast("int"),
                        F.col("account_id").cast("int"),
                        F.col("first_name").cast("string"),
                        F.col("last_name").cast("string"),
                        F.to_date(F.to_timestamp(col=F.col("dob").cast("double")), "yyyy-MM-dd").alias("dob"),
                        F.col("email").cast("string"),
                        F.col("ssn").cast("string"),
                        F.col("occupation").cast("string"),
                        F.col("credit_score").cast("int")
                    ))

# COMMAND ----------

# create upsert class for each deduped df with additional parameter to join on PK ???
class Upsert:
    def __init__(self, name, join_cond, update_temp="stream_updates"):
        self.sql_query = sql_query = f"""
                MERGE INTO silver.{name}_silver a
                USING stream_updates b
                ON {join_cond}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, microBatchDF, batch):
        # display(microBatchDF)
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

accounts_merge = Upsert("accounts", "a.account_id=b.account_id")
customers_merge = Upsert("customers", "a.customer_id=b.customer_id")
address_merge = Upsert("addresses", "a.address_id=b.address_id")
checkings_merge = Upsert("checkings", "a.checkings_id=b.checkings_id")
savings_merge = Upsert("savings", "a.savings_id=b.savings_id")

# COMMAND ----------

mode = "append"

# Upsert silver accounts
query = (accounts_df.writeStream
                   .foreachBatch(accounts_merge.upsert_to_delta)
                   .outputMode(mode)
                   .option("checkpointLocation", f"{checkpoint_dir}/accounts_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# Upsert silver customers
query = (customers_df.writeStream
                   .foreachBatch(customers_merge.upsert_to_delta)
                   .outputMode(mode)
                   .option("checkpointLocation", f"{checkpoint_dir}/customers_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# Upsert silver checkings
query = (checkings_df.writeStream
                   .foreachBatch(checkings_merge.upsert_to_delta)
                   .outputMode(mode)
                   .option("checkpointLocation", f"{checkpoint_dir}/checkings_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# Upsert silver savings
query = (savings_df.writeStream
                   .foreachBatch(savings_merge.upsert_to_delta)
                   .outputMode(mode)
                   .option("checkpointLocation", f"{checkpoint_dir}/savings_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# Upsert silver addresses
query = (addresses_df.writeStream
                   .foreachBatch(address_merge.upsert_to_delta)
                   .outputMode(mode)
                   .option("checkpointLocation", f"{checkpoint_dir}/addresses_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processes

# COMMAND ----------

balancePerState = spark.sql(
    """
    SELECT 
    e.state,
    ROUND(SUM(nvl(b.balance, 0) + nvl(c.balance, 0)),2) total
    FROM  silver.accounts_silver a LEFT JOIN
        silver.savings_silver b ON
            a.savings_id = b.savings_id LEFT JOIN
        silver.checkings_silver c ON
            a.checkings_id = c.checkings_id LEFT JOIN
        silver.customers_silver d ON
            a.account_id = d.account_id LEFT JOIN
        silver.addresses_silver e ON
            d.address_id = e.address_id
    GROUP BY e.state
    """
)

balancePerState.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('gold.daily_balance_per_state')
balancePerState.withColumn('process_date', F.current_timestamp()).write.mode('append').option('mergeSchema', 'true').saveAsTable('gold.historic_balance_per_state')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.daily_balance_per_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.historic_balance_per_state
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   COUNT(customer_id)
# MAGIC FROM bronze.customers_bronze
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(customer_id) > 1;

# COMMAND ----------


