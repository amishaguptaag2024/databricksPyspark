-- Databricks notebook source
select * from TEXT .`/public/retail_db/orders/`

-- COMMAND ----------

 CREATE OR REPLACE TEMPORARY VIEW orders_view (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
 )USING CSV
 OPTIONS (
  path = '/public/retail_db/orders',
  sep = ',',
  header = 'false',
  inferSchema = 'true'
 )

-- COMMAND ----------

DESCRIBE orders_view

-- COMMAND ----------

SELECT * FROM TEXT .`/public/retail_db/order_items/`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items_view (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal DOUBLE, 
  order_item_product_price DOUBLE
  )USING CSV
OPTIONS (
  path = '/public/retail_db/order_items',
  sep = ',',
  header = 'false'
)

-- COMMAND ----------

SELECT * FROM order_items_view;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_join_items AS
  SELECT * 
  FROM orders_view o 
    JOIN order_items_view i
      ON o.order_id = i.order_item_order_id;

-- COMMAND ----------

WITH 
complete_closed_orders AS ( 
  SELECT * 
  FROM orders_join_items
  WHERE order_status IN ('COMPLETE', 'CLOSED')
),
daily_product_total_revenue AS (
  SELECT order_date,
    order_item_product_id,
    ROUND(SUM(order_item_subtotal),2) daily_product_revenue
  FROM complete_closed_orders 
  GROUP BY 1,2
) 
SELECT * 
FROM daily_product_total_revenue 
ORDER BY 1,3 DESC;

-- COMMAND ----------

INSERT OVERWRITE DIRECTORY 'dbfs:/public/retail_db/daily_product_revenue'
USING PARQUET
WITH 
complete_closed_orders AS ( 
  SELECT * 
  FROM orders_join_items
  WHERE order_status IN ('COMPLETE', 'CLOSED')
),
daily_product_total_revenue AS (
  SELECT order_date,
    order_item_product_id,
    ROUND(SUM(order_item_subtotal),2) daily_product_revenue
  FROM complete_closed_orders 
  GROUP BY 1,2
) 
SELECT * 
FROM daily_product_total_revenue 
ORDER BY 1,3 DESC;


-- COMMAND ----------

SELECT * FROM PARQUET .`/public/retail_db/daily_product_revenue/`;

-- COMMAND ----------

