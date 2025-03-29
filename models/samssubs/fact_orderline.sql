{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
o.ORDERDATE as Date_Key,
o.CUSTOMER_ID as Customer_Key,
o.EMPLOYEEEID as Employee_Key,
e.STOREID as Store_Key,
od.PRODUCTID as Product_Key,
dom.OrderMethod_Key,
FROM {{ source('samssubs_landing', 'order') }} as o
INNER JOIN {{ source('samssubs_landing', 'employee') }} as e WHERE o.employeeid = e.employeeid
INNER JOIN {{ source('samssubs_landing', 'orderdetails') }} as od WHERE o.orderid = od.orderid
INNER JOIN dim_ordermethod as dom WHERE o.ordermethod = dom.ordermethod