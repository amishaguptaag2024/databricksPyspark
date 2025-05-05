-- Databricks notebook source
-- MAGIC %fs ls dbfs:/public/retail_db/schemas.json

-- COMMAND ----------

SELECT * FROM TEXT .`dbfs:/public/retail_db/schemas.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.read.text(
-- MAGIC     'dbfs:/public/retail_db/schemas.json',
-- MAGIC     wholetext=True
-- MAGIC ).show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.read.text(
-- MAGIC     'dbfs:/public/retail_db/schemas.json',
-- MAGIC     wholetext=True
-- MAGIC ).first().value
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC schema_text = spark.read.text(
-- MAGIC     'dbfs:/public/retail_db/schemas.json',
-- MAGIC     wholetext=True
-- MAGIC ).first().value

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC type(schema_text)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC import json
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC json.loads(schema_text).keys()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC column_details=json.loads(schema_text)['orders']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sorted(column_details, key=lambda row: row['column_position'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC orders_col=[row['column_name'] for row in column_details]

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/public/retail_db/orders`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv(
-- MAGIC     "dbfs:/public/retail_db/orders",
-- MAGIC     header=False,
-- MAGIC     inferSchema=True,
-- MAGIC     
-- MAGIC )
-- MAGIC
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_with_col=df.toDF(*orders_col)
-- MAGIC
-- MAGIC df_with_col.printSchema()

-- COMMAND ----------

