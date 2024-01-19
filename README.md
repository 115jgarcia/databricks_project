# databricks_project
This project simulates a batch Databricks pipeline for banking data following the medallion architecture. Raw data is created using the Faker library and written as CSV in the cloud storage soluiton Databricks has access to. Raw data is read by Spark into a append-only bronze table for full table history using AutoLoader. A streaming read is then created from the bronze table to only pickup up new records and upserted to a silver table. Bronze records are matched, merged, conformed, and cleansed so that the silver layer can provide an "enterprise view" of all its key entities and transactiosn. Reporting is built upon the silver layer known as the gold layer.

The following features and constraints will be added:
- ~~Create class that generates data and acts as source system.~~ **1/13/2024**
- ~~Use AutoLoader to ingest raw data to bronze.~~ **1/13/2024**
- ~~Add update record creation to source system.~~ **1/15/2024**
- ~~Upsert update records to silver layer.~~ **1/15/2024**
- ~~Add Faker library to install on cluster on start-up.~~ **1/16/2024**
- ~~Create "gold" layer providing potential KPIs.~~ **1/16/2024**
- ~~Update source system sampling of update-records.~~ **1/18/2024**
- Upgrade hive_metastore to Unity Catalog.
- Add dbt to project.
- Add joint account record creation to source system
- Add duplicate record creation to source system.
- Orchestration of jobs in Databricks Workflow.
- Add logic to delete certain records on request.
- Add more tables to data model for possible products seen in banking such as credit cards, mortgages, etc.
- Adapt pipeline from batch to streaming.

Additional features and requirements will be added as task are completed.
