# Databricks notebook source
from faker import Faker
from collections import defaultdict
import random
import datetime

import pyspark.sql.functions as F

fake = Faker(locales='en_US')
Faker.seed(0)

# COMMAND ----------

# MAGIC %scala
# MAGIC // UDFs for updates
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

def randEmail():
    return fake.email()

def randOccupation():
    return fake.job()

def randCreditScore():
    return random.randrange(300,850,1)

def randStrAd():
    return fake.street_address()

def randCity():
    return fake.city()

def randState():
    return fake.state_abbr()

def randZip():
    return fake.postcode()

spark.udf.register("randEmailUdf", randEmail)
spark.udf.register("randOccupationUdf", randOccupation)
spark.udf.register("randCreditScoreUdf", randCreditScore)
spark.udf.register("randStrAdUdf", randStrAd)
spark.udf.register("randCityUdf", randCity)
spark.udf.register("randStateUdf", randState)
spark.udf.register("randZipUdf", randZip)

# COMMAND ----------

class generate_data():
    def __init__(self, current_date):
        self.num_of_records = 1000
        self.unique_customer_id =   fake.unique.random_int(100,10000)
        self.unique_account_id =    fake.unique.random_int(100,10000)
        self.unique_address_id =    fake.unique.random_int(100,10000)
        self.unique_checking_id =   fake.unique.random_int(100,10000)
        self.unique_saving_id =     fake.unique.random_int(100,10000)
        self.start_date =           current_date - datetime.timedelta(days=365)
        self.end_date =             current_date - datetime.timedelta(days=364)
        self.updates_flag =         False
    
    def get_prev_process_date(self):
        return self.end_date - datetime.timedelta(days=1)

    def get_customer_id(self):
        return self.unique_customer_id

    def get_account_id(self):
        return self.unique_account_id

    def get_address_id(self):
        return self.unique_address_id

    def get_checking_id(self):
        return self.unique_checking_id

    def get_saving_id(self):
        return self.unique_saving_id

    def update_unique_id(self):
        inc = 1
        self.unique_customer_id += inc
        self.unique_account_id += inc
        self.unique_address_id += inc
        self.unique_checking_id += inc
        self.unique_saving_id += inc

    def next_date(self):
        self.start_date += datetime.timedelta(days=1)
        self.end_date += datetime.timedelta(days=1)

    def make_negative(self, value, n=1000):
        newValue = value
        if random.randrange(0,n,1) == 0:
            newValue = -newValue
        return newValue

    def create_account_row(self, acc_open_timestamp):
        row = {}
        row['account_id'] =     self.unique_account_id
        row['checking_id'] =    self.unique_checking_id
        row['saving_id'] =      self.unique_saving_id
        row['currency'] =       'USD'
        row['open_date'] =      acc_open_timestamp
        return row
    
    def create_checkings_row(self, acc_open_timestamp):
        row = {}
        row['checking_id'] =            self.unique_checking_id
        row['balance'] =                random.randrange(-50000000, 50000000)/100
        row['open_date'] =              datetime.datetime.timestamp(
                                            fake.date_time_between(
                                                start_date=datetime.datetime.fromtimestamp(acc_open_timestamp),
                                                end_date=self.end_date,
                                                tzinfo = datetime.timezone.utc))
        row['interest_rate'] =          self.make_negative(random.randrange(0,51,1)/1000)   # 1/1000 negative
        row['monthly_fee'] =            self.make_negative(random.randrange(0,26,25))       # 1/1000 negative
        row['routing_number'] =         fake.aba()
        row['account_number'] =         fake.iban()
        row['overdraft_protection'] =   fake.bothify(text='?', letters='YN')
        row['is_active'] =              fake.bothify(text='?', letters='YN')
        return row

    def create_savings_row(self, acc_open_timestamp):
        row = {}
        row['saving_id'] =              self.unique_saving_id
        row['balance'] =                random.randrange(100000, 100000000)/100
        row['open_date'] =              datetime.datetime.timestamp(
                                            fake.date_time_between(
                                                start_date=datetime.datetime.fromtimestamp(acc_open_timestamp),
                                                end_date=self.end_date,
                                                tzinfo = datetime.timezone.utc))
        row['interest_rate'] =          self.make_negative(random.randrange(0,51,1)/1000)       # 1/1000 negative
        row['deposit_limit'] =          self.make_negative(random.randrange(5000,10001,1000))   # 1/1000 negative
        row['routing_number'] =         fake.aba()
        row['account_number'] =         fake.iban()
        row['overdraft_protection'] =   fake.bothify(text='?', letters='YN')
        row['is_active'] =              fake.bothify(text='?', letters='YN')
        return row

    def create_customer_row(self):
        row = {}
        row['customer_id'] =    self.unique_customer_id
        row['address_id'] =     self.unique_address_id
        row['account_id'] =     self.unique_account_id
        row['first_name'] =     fake.first_name()
        row['last_name'] =      fake.last_name()
        row['dob'] =            fake.date_of_birth(minimum_age=18)
        row['email'] =          fake.email()
        row['occupation'] =     fake.job()
        row['ssn'] =            fake.ssn()
        row['credit_score'] =   random.randrange(280,851,1)
        return row

    def create_address_row(self):
        row = {}
        row['address_id'] =     self.unique_address_id
        row['address_line'] =   fake.street_address()
        row['city'] =           fake.city()
        row['state'] =          fake.state_abbr()
        row['zipcode'] =        fake.postcode()
        return row

    def create_all_tables(self):
        dataset = defaultdict(list)

        for i in range(0,self.num_of_records):
            account_open_date = datetime.datetime.timestamp(
                                    fake.date_time_between(
                                        start_date=self.start_date,
                                        end_date=self.end_date,
                                        tzinfo=datetime.timezone.utc)
                                    )
            
            # checkings_table
            row = self.create_checkings_row(account_open_date)
            dataset['checkings'].append(row)
            
            # savings_table
            row = self.create_savings_row(account_open_date)
            dataset['savings'].append(row)
            
            # customer_table
            row = self.create_customer_row()
            dataset['customers'].append(row)
            
            # address_table
            row = self.create_address_row()
            dataset['addresses'].append(row)
            
            # account_table
            row = self.create_account_row(account_open_date)
            dataset['accounts'].append(row)

            # update unique id
            self.update_unique_id()
        
        self.next_date()

        return dataset
    
    def updates_checkings(self, newRecords, tableName):
        if self.updates_flag:
            spark.read.table("bronze.checkings_bronze").filter("is_active = 'Y'").sample(fraction=0.01, seed=0).createOrReplaceTempView("_tempUpdateCreation")

            df = spark.sql(
                """
                SELECT account_number, randBalUdf(balance) as balance, checking_id, randIntRateUdf(interest_rate) as interest_rate, is_active, monthly_fee, open_date, overdraft_protection, routing_number
                FROM _tempUpdateCreation;
                """
            )
            df = df.limit(1).union(df)
            newRecords = newRecords.union(df)
        (newRecords.coalesce(1).write.format('csv')
                .option('header', 'true')
                .option('delimiter', '|')
                .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(self.end_date)) + '_' + tableName}"))
                # .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + tableName}"))
    
    def updates_savings(self, newRecords, tableName):
        if self.updates_flag:
            spark.read.table("bronze.savings_bronze").filter("is_active = 'Y'").sample(fraction=0.01, seed=0).createOrReplaceTempView("_tempUpdateCreation")

            df = spark.sql(
                """
                SELECT account_number, randBalUdf(balance) as balance, deposit_limit, randIntRateUdf(interest_rate) as interest_rate, is_active, open_date, overdraft_protection, routing_number, saving_id
                FROM _tempUpdateCreation;
                """
            )
            df = df.limit(1).union(df)
            newRecords = newRecords.union(df)
        (newRecords.coalesce(1).write.format('csv')
                .option('header', 'true')
                .option('delimiter', '|')
                .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(self.end_date)) + '_' + tableName}"))
                # .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + tableName}"))

    def updates_addresses(self, newRecords, tableName):
        if self.updates_flag:
            spark.read.table("bronze.addresses_bronze").sample(fraction=0.01, seed=0).createOrReplaceTempView("_tempUpdateCreation")

            df = spark.sql(
                """
                SELECT address_id, randStrAdUdf() as address_line, randCityUdf() as city, randStateUdf() as state, randZipUdf() as zipcode
                FROM _tempUpdateCreation;
                """
            )
            df = df.limit(1).union(df)
            newRecords = newRecords.union(df)
        (newRecords.coalesce(1).write.format('csv')
                .option('header', 'true')
                .option('delimiter', '|')
                .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(self.end_date)) + '_' + tableName}"))
                # .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + tableName}"))

    def updates_customers(self, newRecords, tableName):
        if self.updates_flag:
            spark.read.table("bronze.customers_bronze").sample(fraction=0.01, seed=0).createOrReplaceTempView("_tempUpdateCreation")

            df = spark.sql(
                """
                SELECT account_id, address_id, randCreditScoreUdf() as credit_score, customer_id, dob, randEmailUdf() as email, first_name, last_name, randOccupationUdf() as occupation, ssn
                FROM _tempUpdateCreation;
                """
            )
            df = df.limit(1).union(df)
            newRecords = newRecords.union(df)
        (newRecords.coalesce(1).write.format('csv')
                .option('header', 'true')
                .option('delimiter', '|')
                .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(self.end_date)) + '_' + tableName}"))
                # .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + tableName}"))

    def updates_accounts(self, newRecords, tableName):
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
        
        newRecords.createOrReplaceTempView('tempAccounts')

        newRecords = spark.sql(
            """
            SELECT
                account_id, tempCheckings.checking_id, tempSavings.saving_id, currency, tempAccounts.open_date
            FROM
                tempAccounts LEFT JOIN tempSavings
                    ON tempAccounts.saving_id = tempSavings.saving_id
                LEFT JOIN tempCheckings
                    ON tempAccounts.checking_id = tempCheckings.checking_id
            """
        )

        spark.catalog.dropTempView('tempSavings')
        spark.catalog.dropTempView('tempCheckings')
        spark.catalog.dropTempView('tempAccounts')

        (newRecords.coalesce(1).write.format('csv')
                .option('header', 'true')
                .option('delimiter', '|')
                .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(self.end_date)) + '_' + tableName}"))
                # .save(f"gs://bankdatajg/generator/{str(datetime.datetime.timestamp(datetime.datetime.now())) + '_' + tableName}"))

    def write_data(self, dataset):
        if dataset is None:
            print("Nothing was written. :D")
        else:
            for i in dataset:
                df = spark.createDataFrame(dataset[i])
                if i == 'savings':
                    self.updates_savings(df.sample(fraction=0.5), i)
                elif i == 'checkings':
                    self.updates_checkings(df.sample(fraction=0.7), i)
                elif i == 'accounts':
                    self.updates_accounts(df, i)
                elif i == 'customers':
                    self.updates_customers(df, i)
                elif i == 'addresses':
                    self.updates_addresses(df, i)           
            if not self.updates_flag: self.updates_flag = True

# COMMAND ----------

# print(f"Executor cores: {sc.defaultParallelism}")
# spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
