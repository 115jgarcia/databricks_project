# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.accounts_silver (
# MAGIC     account_id INT,
# MAGIC     checking_id INT,
# MAGIC     saving_id INT,
# MAGIC     currency STRING,
# MAGIC     open_date DATE,
# MAGIC     process_date DATE
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/accounts_silver'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers_silver (
# MAGIC     customer_id INT,
# MAGIC     address_id INT,
# MAGIC     account_id INT, 
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     dob DATE,
# MAGIC     email STRING,
# MAGIC     ssn STRING,
# MAGIC     occupation STRING,
# MAGIC     credit_score INT,
# MAGIC     process_date DATE
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/customers_silver'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.addresses_silver (
# MAGIC     address_id INT,
# MAGIC     address_line STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zipcode INT,
# MAGIC     process_date DATE
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/addresses_silver'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.checkings_silver (
# MAGIC     checking_id INT,
# MAGIC     balance DOUBLE,
# MAGIC     open_date DATE,
# MAGIC     interest_rate DOUBLE,
# MAGIC     monthly_fee DOUBLE,
# MAGIC     routing_number STRING,
# MAGIC     account_number STRING,
# MAGIC     overdraft_protection STRING,
# MAGIC     is_active STRING,
# MAGIC     process_date DATE
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/checkings_silver'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.savings_silver (
# MAGIC     saving_id INT,
# MAGIC     balance DOUBLE,
# MAGIC     open_date DATE,
# MAGIC     interest_rate DOUBLE,
# MAGIC     deposit_limit DOUBLE,
# MAGIC     routing_number STRING,
# MAGIC     account_number STRING,
# MAGIC     overdraft_protection STRING,
# MAGIC     is_active STRING,
# MAGIC     process_date DATE
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/savings_silver'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.quarantine_data (
# MAGIC     pk STRING,
# MAGIC     table_name STRING,
# MAGIC     file_name STRING,
# MAGIC     flag STRING
# MAGIC )
# MAGIC LOCATION 'gs://bankdatajg/silver/quarantine_data'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
