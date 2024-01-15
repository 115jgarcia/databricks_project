# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.accounts_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/accounts_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers_bronze(
# MAGIC   customer_id STRING,
# MAGIC   address_id STRING,
# MAGIC   account_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   dob STRING,
# MAGIC   email STRING,
# MAGIC   ssn STRING,
# MAGIC   occupation STRING,
# MAGIC   credit_score STRING,
# MAGIC   filename STRING,
# MAGIC   process_date TIMESTAMP
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/bronze/customers_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.addresses_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/addresses_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.checkings_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/checkings_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.savings_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/savings_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


