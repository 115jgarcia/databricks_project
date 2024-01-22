# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.daily_balance_per_state (
# MAGIC     state STRING,
# MAGIC     total DOUBLE
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.historic_balance_per_state (
# MAGIC     state STRING,
# MAGIC     total DOUBLE,
# MAGIC     process_date TIMESTAMP
# MAGIC );
