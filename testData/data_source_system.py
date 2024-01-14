# Databricks notebook source
# MAGIC %md
# MAGIC Using Faker Library to generate dummy data.
# MAGIC Install Link: https://faker.readthedocs.io/en/master/

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from faker import Faker
import random
import csv
import datetime

import pyspark.sql.functions as F

fake = Faker(locales='en_US')
Faker.seed(0)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{udf}
# MAGIC
# MAGIC val randBal = (s: Double) => {
# MAGIC   val rand = new scala.util.Random
# MAGIC   (-50000000 + rand.nextInt((50000000 + 50000000) + 1)).toDouble / 100
# MAGIC }
# MAGIC
# MAGIC val randIntRate = (s: Double) => {
# MAGIC   val rand = new scala.util.Random
# MAGIC   (rand.nextInt(50).toDouble)/1000
# MAGIC }
# MAGIC
# MAGIC spark.udf.register("randBalUdf", randBal)
# MAGIC spark.udf.register("randIntRateUdf", randIntRate)

# COMMAND ----------

class generate_data():
    def __init__(self):
        self.unique_customer_id =   fake.unique.random_int(100,10000)
        self.unique_account_id =    fake.unique.random_int(100,10000)
        self.unique_address_id =    fake.unique.random_int(100,10000)
        self.unique_checkings_id =  fake.unique.random_int(100,10000)
        self.unique_savings_id =    fake.unique.random_int(100,10000)
        self.updates_flag = False
    
    def get_customer_id(self):
        return self.unique_customer_id

    def get_account_id(self):
        return self.unique_account_id

    def get_address_id(self):
        return self.unique_address_id

    def get_checkings_id(self):
        return self.unique_checkings_id

    def get_savings_id(self):
        return self.unique_savings_id

    def update_unique_id(self):
        inc = 1
        
        self.unique_customer_id += inc
        self.unique_account_id += inc
        self.unique_address_id += inc
        self.unique_checkings_id += inc
        self.unique_savings_id += inc

    def create_account_row(self, timestamp):
        row = {}
        row['account_id'] =     self.unique_account_id
        row['checkings_id'] =   self.unique_checkings_id
        row['savings_id'] =     self.unique_savings_id
        row['currency'] =       'USD'
        row['open_date'] =      timestamp
        return row
    
    def create_checkings_row(self, acc_timestamp):
        row = {}
        row['checkings_id'] =           self.unique_checkings_id
        row['balance'] =                random.randrange(-50000000, 50000000)/100
        row['open_date'] =              datetime.datetime.timestamp(fake.date_time_between(start_date=datetime.datetime.fromtimestamp(acc_timestamp),tzinfo = datetime.timezone.utc))
        row['interest_rate'] =          random.randrange(0,50,1)/1000
        row['monthly_fee'] =            random.randrange(0,25,25)
        row['routing_number'] =         fake.aba()
        row['account_number'] =         fake.iban()
        row['overdraft_protection'] =   fake.bothify(text='?', letters='YN')
        row['is_active'] =              fake.bothify(text='?', letters='YN')
        return row

    def create_savings_row(self, acc_timestamp):
        row = {}
        row['savings_id'] =             self.unique_savings_id
        row['balance'] =                random.randrange(100000, 100000000)/100
        row['open_date'] =              datetime.datetime.timestamp(fake.date_time_between(start_date=datetime.datetime.fromtimestamp(acc_timestamp),tzinfo = datetime.timezone.utc))
        row['interest_rate'] =          random.randrange(0,50,1)/1000
        row['deposit_limit'] =          random.randrange(5000,10000,1000)
        row['routing_number'] =         fake.aba()
        row['account_number'] =         fake.iban()
        row['overdraft_protection'] =   fake.bothify(text='?', letters='YN')
        row['is_active'] =              fake.bothify(text='?', letters='YN')
        return row

    def create_customer_row(self):
        row = {}
        row['customer_id'] =    self.unique_customer_id
        row['address_id'] =     self.unique_address_id
        row['account_id'] =     self.unique_customer_id
        row['first_name'] =     fake.first_name()
        row['last_name'] =      fake.last_name()
        row['dob'] =            fake.date_of_birth(minimum_age=18)
        row['email'] =          fake.email()
        row['occupation'] =     fake.job()
        row['ssn'] =            fake.ssn()
        row['credit_score'] =   random.randrange(300,850,1)
        return row

    def create_address_row(self):
        row = {}
        row['address_id'] =     self.unique_address_id
        row['address_line'] =   fake.street_address()
        row['city'] =           fake.city()
        row['state'] =          fake.state_abbr()
        row['zipcode'] =        fake.postcode()
        return row

    def create_all_tables(self, n=1000):
        dataset = [[],[],[],[],[]]

        # add logic for updates flag. after 1st run, change to True. use spark.sql to create updates and union. 

        for i in range(0,n):
            account_open_date = datetime.datetime.timestamp(fake.date_time_between(tzinfo = datetime.timezone.utc))
            
            # checkings_table
            row = self.create_checkings_row(account_open_date)
            dataset[0].append(row)
            
            # savings_table
            row = self.create_savings_row(account_open_date)
            dataset[1].append(row)
            
            # customer_table
            row = self.create_customer_row()
            dataset[2].append(row)
            
            # address_table
            row = self.create_address_row()
            dataset[3].append(row)
            
            # account_table
            row = self.create_account_row(account_open_date)
            dataset[4].append(row)

            # update unique id
            self.update_unique_id()
        
        return dataset
    
    def updates_checkings(self):
        pat
        updates = spark.sql(
            """
            SELECT account_number, randBalUdf(balance), chackings_idf, randIntRateUdf(interest_rate) as interest_rate, is_active, monthly_fee, open_date, overdraft_protection, routing_number
            FROM bronze.checkings_bronze
            WHERE is_active = 'Y'
            LIMIT 10;
            """
        )

        updates.write.
        return
    
    def updates_savings(self):
        return
    
    def updates_addresses(self):
        return
    
    def update_customers(self):
        return

    def create_updates(self):
        # create checkings updates
        self.updates_checkings()
    
def write_data(dataset={}):
    j = 0
    dataName = ['checkings', 'savings', 'customers', 'addresses','accounts']
    if dataset == {}:
        print("Nothing was written. :D")
    else:
        for i in dataset:
            df = spark.createDataFrame(dataset[j])
            
            if dataName[j] == 'savings':
                df = df.sample(fraction=0.5)
                (df.coalesce(1).write.format('csv')
                        .option('header', 'true')
                        .option('delimiter', '|')
                        .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + dataName[j]}"))
            elif dataName[j] == 'checkings':
                df = df.sample(fraction=0.7)
                (df.coalesce(1).write.format('csv')
                        .option('header', 'true')
                        .option('delimiter', '|')
                        .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + dataName[j]}"))
            elif dataName[j] == 'accounts':
                savings_path = ''
                checkings_path = ''

                files = dbutils.fs.ls('gs://bankdatajg/generator/')

                for a in files:
                    if 'savings' in a.path:
                        savings_path = a.path
                    elif 'checkings' in a.path:
                        checkings_path = a.path

                spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(savings_path).createOrReplaceTempView('tempSavings')
                spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(checkings_path).createOrReplaceTempView('tempCheckings')
                
                df.createOrReplaceTempView('tempAccounts')

                df = spark.sql(
                    """
                    SELECT
                        account_id, tempCheckings.checkings_id, tempSavings.savings_id, currency, tempAccounts.open_date
                    FROM
                        tempAccounts LEFT JOIN tempSavings
                            ON tempAccounts.savings_id = tempSavings.savings_id
                        LEFT JOIN tempCheckings
                            ON tempAccounts.checkings_id = tempCheckings.checkings_id
                    """
                )

                spark.catalog.dropTempView('tempSavings')
                spark.catalog.dropTempView('tempCheckings')
                spark.catalog.dropTempView('tempAccounts')

                (df.coalesce(1).write.format('csv')
                        .option('header', 'true')
                        .option('delimiter', '|')
                        .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + dataName[j]}"))
            else:
                (df.coalesce(1).write.format('csv')
                        .option('header', 'true')
                        .option('delimiter', '|')
                        .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + dataName[j]}"))
            j+=1

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

data_generator = generate_data()
dataset = data_generator.create_all_tables()

# COMMAND ----------

df = spark.createDataFrame(dataset[0])
df.printSchema()

# COMMAND ----------

samp_df = df.filter(df.is_active =='Y').sample(fraction=0.01)
samp_df.createOrReplaceTempView('samp_df')

# COMMAND ----------

import pyspark.sql.types as T

# COMMAND ----------

# %scala
# import org.apache.spark.sql.functions.{udf}

# val randBalScala = (s: Double) => {
#   val rand = new scala.util.Random
#   (-50000000 + rand.nextInt((50000000 + 50000000) + 1)).toDouble / 100
# }
# spark.udf.register("randBalScalaUdf", randBalScala)

# COMMAND ----------

checkings_br = spark.sql(
    """
    SELECT
        account_number, checkings_id, randBalScalaUdf(balance) as balance, interest_rate, is_active, monthly_fee, open_date, overdraft_protection, routing_number
    FROM bronze.checkings_bronze
    WHERE
        is_active = 'Y'
    LIMIT 10;
    """
)

# COMMAND ----------

display(checkings_br)

# COMMAND ----------

display(df)

# COMMAND ----------

updates = df.union(checkings_br)

# COMMAND ----------

display(updates)

# COMMAND ----------

spark.sql(
    """
    SELECT *
    FROM
        bronze.notRead
    """
)

# COMMAND ----------


