{{config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
customer_id as customer_key,
customer_id as customerid,
first_name as firstname,
last_name as lastname,
email,
phone_number as phonenumber,
state
FROM {{ source('oliver_landing', 'customer') }}