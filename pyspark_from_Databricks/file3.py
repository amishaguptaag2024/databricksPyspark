# Databricks notebook source
df = spark.read.csv(
    "dbfs:/public/retail_db/orders",
    header=False,
    inferSchema=True,
    
)


# COMMAND ----------

schema_text = spark.read.text(
    'dbfs:/public/retail_db/schemas.json',
    wholetext=True
).first().value

# COMMAND ----------

import json

# COMMAND ----------

column_details=json.loads(schema_text)['orders']

orders_col=[row['column_name'] for row in column_details]

# COMMAND ----------

ordersDF=df.toDF(*orders_col)

# COMMAND ----------

ordersDF.printSchema()

# COMMAND ----------

#count the order_status and order in desc of count
from pyspark.sql.functions import count,desc

ordersDF.groupBy(ordersDF.order_status).agg(count(ordersDF.order_status).alias('status_count')).orderBy(desc('status_count')).display()

# COMMAND ----------

