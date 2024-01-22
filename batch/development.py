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

today = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
data_generator = generate_data(today)

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
                .withColumn("file_name", F.input_file_name())
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

accounts_df = (spark.read
                .table("bronze.accounts_bronze")
                .dropDuplicates(["account_id", "process_date"])
                .select(
                    F.col("account_id").cast("int"),
                    F.col("checking_id").cast("int"),
                    F.col("saving_id").cast("int"),
                    F.col("currency").cast("string"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("file_name"),
                    F.lit(None).alias("flag"))
                )
display(accounts_df)

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
                .dropDuplicates(["account_id", "process_date"])
                .select(
                    F.col("account_id").cast("int"),
                    F.col("checking_id").cast("int"),
                    F.col("saving_id").cast("int"),
                    F.col("currency").cast("string"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("file_name"),
                    F.lit(None).alias("flag"))
                )

checkings_df = (spark.readStream
                .table("bronze.checkings_bronze")
                .dropDuplicates(["checking_id", "process_date"])
                .select(
                    F.col("checking_id").cast("int"),
                    F.col("balance").cast("double"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("interest_rate").cast("double"),
                    F.col("monthly_fee").cast("double"),
                    F.col("routing_number").cast("string"),
                    F.col("account_number").cast("string"),
                    F.col("overdraft_protection").cast("string"),
                    F.col("is_active").cast("string"),
                    F.col("file_name"),
                    F.when((F.col("monthly_fee") < 0) |         # Data quality checks
                           (F.col("interest_rate") < 0.0), 
                           "Failed data quality check.").alias("flag"))
                )

savings_df = (spark.readStream
                .table("bronze.savings_bronze")
                .dropDuplicates(["saving_id", "process_date"])
                .select(
                    F.col("saving_id").cast("int"),
                    F.col("balance").cast("double"),
                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                    F.col("interest_rate").cast("double"),
                    F.col("deposit_limit").cast("double"),
                    F.col("routing_number").cast("string"),
                    F.col("account_number").cast("string"),
                    F.col("overdraft_protection").cast("string"),
                    F.col("is_active").cast("string"),
                    F.col("file_name"),
                    F.when((F.col("interest_rate") < 0.0) |     # Data quality checks
                           (F.col("deposit_limit") < 0)
                           , "Failed data quality check.").alias("flag"))
                )

addresses_df = (spark.readStream
                    .table("bronze.addresses_bronze")
                    .dropDuplicates(["address_id", "process_date"])
                    .select(
                        F.col("address_id").cast("int"),
                        F.col("address_line").cast("string"),
                        F.col("city").cast("string"),
                        F.col("state").cast("string"),
                        F.col("zipcode").cast("int"),
                        F.col("file_name"),
                        F.lit(None).alias("flag"))
                    )

customers_df = (spark.readStream
                    .table("bronze.customers_bronze")
                    .dropDuplicates(["customer_id", "process_date"])
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
                        F.col("credit_score").cast("int"),
                        F.col("file_name"),
                        F.when((F.col("credit_score") < 300)    # Data quality checks
                               , "Failed data quality check.").alias("flag"))
                    )

# COMMAND ----------

# create upsert class for each deduped df with additional parameter to join on PK ???
class Upsert:
    def __init__(self, name, pk, join_cond, update_temp="stream_updates"):
        self.sql_query = sql_query = f"""
                MERGE INTO silver.{name}_silver a
                USING stream_updates b
                ON {join_cond}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
        self.update_temp = update_temp
        self.name = name
        self.pk = pk
        
    def upsert_to_delta(self, microBatchDF, batch):
        # display(microBatchDF.groupBy('saving_id')
        #         .agg(F.count('saving_id').alias('cnt'))
        #         .filter(F.col('cnt')>1)
        #         .select(F.col('saving_id'))
        #         )
        microBatchDF.filter("flag IS NULL").drop("file_name", "flag").createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)
        (microBatchDF
            .filter("flag IS NOT NULL")
            .select(F.col(f"{self.pk}_id").cast("string").alias("pk"), F.lit(f"{self.name}_bronze").alias("table_name"), "file_name", "flag")
            .write.format("delta").mode("append").saveAsTable("silver.quarantine_data"))

accounts_merge = Upsert("accounts", "account", "a.account_id=b.account_id")
customers_merge = Upsert("customers", "customer", "a.customer_id=b.customer_id")
address_merge = Upsert("addresses", "address", "a.address_id=b.address_id")
checkings_merge = Upsert("checkings", "checking", "a.checking_id=b.checking_id")
savings_merge = Upsert("savings", "saving", "a.saving_id=b.saving_id")

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
            a.saving_id = b.saving_id LEFT JOIN
        silver.checkings_silver c ON
            a.checking_id = c.checking_id LEFT JOIN
        silver.customers_silver d ON
            a.account_id = d.account_id LEFT JOIN
        silver.addresses_silver e ON
            d.address_id = e.address_id
    GROUP BY e.state
    """
).cache()

balancePerState.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('gold.daily_balance_per_state')
balancePerState.withColumn('process_date', F.current_timestamp()).write.mode('append').option('mergeSchema', 'true').saveAsTable('gold.historic_balance_per_state')

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries

# COMMAND ----------


