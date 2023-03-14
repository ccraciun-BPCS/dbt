-- Create a table called "sales_data"
{{
  config(
    materialized='table',
    schema='my_schema',
    unique_key='order_id'
  )
}}

CREATE TABLE {{ ref('raw_sales_data') }} (
  order_id INTEGER,
  customer_id INTEGER,
  order_date TIMESTAMP,
  product_id INTEGER,
  quantity INTEGER,
  price DECIMAL(10, 2)
);

-- Transform the data by calculating the total revenue for each order
SELECT
  order_id,
  SUM(quantity * price) AS total_revenue
FROM {{ ref('raw_sales_data') }}
GROUP BY 1;

-- Create a new table called "sales_summary" with the transformed data
{{
  config(
    materialized='table',
    schema='my_schema',
    unique_key='order_id'
  )
}}

CREATE TABLE {{ ref('sales_summary') }} (
  order_id INTEGER,
  total_revenue DECIMAL(10, 2)
);

INSERT INTO {{ ref('sales_summary') }} (order_id, total_revenue)
{{ run_query('SELECT * FROM {{ ref('transformed_sales_data') }}') }};
