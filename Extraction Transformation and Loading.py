# Databricks notebook source

storage_account_name = 'himanshudatabricks'
container_name = 'source'
access_key = 'jGz4++lECt93qX/xrQV9gi0J4WlZWBxLujNcNXewK3VGPjhwFdRtu9B0U4yigAyuqHdASRfkKxzk+ASt1pJvIQ=='
# set access key in spark config

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)

#Load files into dataframes one by one
df2020q1 = spark.read.csv(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/IT_Services_Marketshare_2020Q1.CSV", inferSchema = True, header = True)

df2020 = spark.read.csv(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/IT_Services_Marketshare_2020.CSV", inferSchema = True, header = True)


# COMMAND ----------

df2020q1.display()

# COMMAND ----------

df2020.display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
dftemp = df2020q1.withColumnRenamed('Vendor Name', 'Vendor')

# COMMAND ----------

df = dftemp.unionByName(df2020)
display(df)

# COMMAND ----------

df = df.withColumn('Year', split('Year', ' ').getItem(0))

# COMMAND ----------

spark.sql('create database ETLdb')

# COMMAND ----------

spark.sql('DROP database etldb')

# COMMAND ----------

spark.sql('create database ETLdb')

# COMMAND ----------

spark.sql("use etldb")
df.write.format('csv').mode('append').saveAsTable('marketshare3')

# COMMAND ----------

spark.sql('show tables').show()

# COMMAND ----------

dt = df.groupBy('Country', 'Vertical', 'Vendor', 'Year').agg(sum('VendorRevenue - USD').alias('VendorRevenue - USD'))


# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('Vendor').orderBy(desc('VendorRevenue - USD'))
result = dt.withColumn('Rank', rank().over(window)).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select Vendor, sum(`VendorRevenue - USD`), min(`Year`)  from etldb.marketshare3 where `Year` = "2019" group by(`vendor`)

# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('Vendor').orderBy('VendorRevenue - USD')
new_df= df.withColumn('Rank', dense_rank().over(window))
new_df.filter(col('Rank') < 11).display()

