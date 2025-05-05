# Databricks notebook source
import json

# COMMAND ----------

def get_columns(schemas_file,ds_name):
    schema_text=spark.read.text(schemas_file, wholetext=True).first().value
    schemas=json.loads(schema_text)
    column_details=schemas[ds_name]
    return [column['column_name'] for column in sorted(column_details,key=lambda x: x['column_position'])]


# COMMAND ----------

column=get_columns('/public/retail_db/schemas.json','orders')
column

# COMMAND ----------

ds_list=[
    'departments', 'categories', 'products', 'customers', 'orders', 'order_items'
]

# COMMAND ----------

base_dir='dbfs:/public/retail_db'

# COMMAND ----------

tgt_dir='dbfs:/public/retail_db_parquet'

# COMMAND ----------

for ds_name in ds_list:
    print(f'Processing {ds_name} data')
    columns=get_columns(f'{base_dir}/schemas.json',ds_name)
    df=spark.read.csv(f'{base_dir}/{ds_name}',header=True,inferSchema=True).toDF(*columns)
    df.\
        write.\
        mode('overwrite').\
        parquet(f'{tgt_dir}/{ds_name}')


# COMMAND ----------

for ds_name in ds_list:
    print(f'Processing {ds_name} data')
    df=spark.read.parquet(f'{tgt_dir}/{ds_name}')   
    df.show(3)

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_parquet/orders

# COMMAND ----------

