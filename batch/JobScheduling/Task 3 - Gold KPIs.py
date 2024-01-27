# Databricks notebook source
import pyspark.sql.functions as F

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

balancePerState = spark.sql(
    """
    SELECT 
    e.state,
    ROUND(SUM(nvl(b.balance, 0) + nvl(c.balance, 0)),2) total,
    e.process_date
    FROM  silver.accounts_silver a LEFT JOIN
        silver.savings_silver b ON
            a.saving_id = b.saving_id LEFT JOIN
        silver.checkings_silver c ON
            a.checking_id = c.checking_id LEFT JOIN
        silver.customers_silver d ON
            a.account_id = d.account_id RIGHT JOIN
        silver.addresses_silver e ON
            d.address_id = e.address_id
    GROUP BY e.state, e.process_date;
    """
).cache()

balancePerState.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('gold.daily_balance_per_state')
balancePerState.write.mode('append').option('mergeSchema', 'true').saveAsTable('gold.historic_balance_per_state')
