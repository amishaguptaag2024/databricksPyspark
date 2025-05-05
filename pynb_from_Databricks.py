# Databricks notebook source
print('hello world')

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

print("linked vsCode with databricksCluster")

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db/orders

# COMMAND ----------

spark.sql('select current_date()').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW orders (
# MAGIC   order_id INT,
# MAGIC   order_date DATE,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) USING CSV 
# MAGIC OPTIONS  (path = "dbfs:/public/retail_db/orders/"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_status,
# MAGIC   count(*) as status_count 
# MAGIC from orders
# MAGIC group by 1
# MAGIC order by 2
# MAGIC ;

# COMMAND ----------

