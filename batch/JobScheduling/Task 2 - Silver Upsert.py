# Databricks notebook source
import pyspark.sql.functions as F

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# checkpoint directory
checkpoint_dir = "gs://bankdatajg/checkpoint"

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
        microBatchDF.filter("flag IS NULL").drop("file_name", "flag").createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)
        (microBatchDF
            .filter("flag IS NOT NULL")
            .select(F.col(f"{self.pk}").cast("string").alias("pk"), F.lit(f"{self.name}_bronze").alias("table_name"), "file_name", "flag")
            .write.format("delta").mode("append").saveAsTable("silver.quarantine_data"))

mode = "append"

table_config = {
#   table_name  : [pk,              join_cond]
    'accounts'  : ["account_id",    "a.account_id=b.account_id"],
    'checkings' : ["checking_id",   "a.checking_id=b.checking_id"],
    'savings'   : ["saving_id",     "a.saving_id=b.saving_id"],
    'addresses' : ["address_id",    "a.address_id=b.address_id"],
    'customers' : ["customer_id",   "a.customer_id=b.customer_id"] 
}

for i,k in table_config.items():
    table_config[i].append(
        {
            f"{i}_merge"    :   Upsert(i, k[0], k[1])
        }
    )

table_config['accounts'].append(
                            (spark.readStream
                                .table("bronze.accounts_bronze")
                                .dropDuplicates(["account_id", "process_date"])
                                .select(
                                    F.col("account_id").cast("int"),
                                    F.col("checking_id").cast("int"),
                                    F.col("saving_id").cast("int"),
                                    F.col("currency").cast("string"),
                                    F.to_date(F.to_timestamp(col=F.col("open_date").cast("double")), "yyyy-MM-dd").alias("open_date"),
                                    F.to_date(F.to_timestamp(col=F.col("process_date").cast("double")), "yyyy-MM-dd").alias("process_date"),
                                    F.col("file_name"),
                                    F.lit(None).alias("flag"))
                                )
                            )

table_config['checkings'].append(
                            (spark.readStream
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
                                    F.to_date(F.to_timestamp(col=F.col("process_date").cast("double")), "yyyy-MM-dd").alias("process_date"),
                                    F.col("file_name"),
                                    F.when((F.col("monthly_fee") < 0) |         # Data quality checks
                                        (F.col("interest_rate") < 0.0), 
                                        "Failed data quality check.").alias("flag"))
                                )
                            )

table_config['savings'].append(
                            (spark.readStream
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
                                    F.to_date(F.to_timestamp(col=F.col("process_date").cast("double")), "yyyy-MM-dd").alias("process_date"),
                                    F.col("file_name"),
                                    F.when((F.col("interest_rate") < 0.0) |     # Data quality checks
                                        (F.col("deposit_limit") < 0)
                                        , "Failed data quality check.").alias("flag"))
                                )
                            )

table_config['addresses'].append(
                            (spark.readStream
                                .table("bronze.addresses_bronze")
                                .dropDuplicates(["address_id", "process_date"])
                                .select(
                                    F.col("address_id").cast("int"),
                                    F.col("address_line").cast("string"),
                                    F.col("city").cast("string"),
                                    F.col("state").cast("string"),
                                    F.col("zipcode").cast("int"),
                                    F.to_date(F.to_timestamp(col=F.col("process_date").cast("double")), "yyyy-MM-dd").alias("process_date"),
                                    F.col("file_name"),
                                    F.lit(None).alias("flag"))
                                )
                            )

table_config['customers'].append(
                            (spark.readStream.table("bronze.customers_bronze")
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
                                    F.to_date(F.to_timestamp(col=F.col("process_date").cast("double")), "yyyy-MM-dd").alias("process_date"),
                                    F.col("file_name"),
                                    F.when((F.col("credit_score") < 300)    # Data quality checks
                                        , "Failed data quality check.").alias("flag"))
                                )
                            )

for key, value in table_config.items():
    (value[3].writeStream
            .foreachBatch(value[2][f"{key}_merge"].upsert_to_delta)
            .outputMode(mode)
            .option("checkpointLoation", f"{checkpoint_dir}/{key}_silver")
            .trigger(availableNow=True)
            .start()
        ).awaitTermination()
