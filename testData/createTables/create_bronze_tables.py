# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.accounts_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/accounts_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/customers_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.addresses_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/addresses_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.checkings_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/checkings_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.savings_bronze
# MAGIC LOCATION 'gs://bankdatajg/bronze/savings_bronze'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
