{{config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
o.customer_id as Cust_Key,
o.order_date as Date_Key,
o.store_id as Store_Key,
do.product_id as Product_Key,
o.employee_id as Employee_Key,
ol.quantity as Quantity,
o.total_amount as Dollars_Sold,
ol.unit_price as Unit_Price,
FROM {{ source('oliver_landing', 'orders') }} o
INNER JOIN {{ ref('oliver_dim_customer') }} dc ON dc.customer_key = o.customer_id
INNER JOIN {{ ref('oliver_dim_date') }} dd ON dd.date_key = o.order_date
INNER JOIN {{ ref('oliver_dim_store') }} ds ON ds.store_key = o.store_id
INNER JOIN {{ ref('oliver_dim_employee') }} de ON de.employee_key = o.employee_id
INNER JOIN {{ source('oliver_landing', 'orderline') }} ol ON ol.order_id = o.order_id
INNER JOIN {{ ref('oliver_dim_product') }} do ON do.product_key = ol.product_id