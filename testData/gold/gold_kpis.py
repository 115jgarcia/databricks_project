# Databricks notebook source
import pyspark.sql.functions as F

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
