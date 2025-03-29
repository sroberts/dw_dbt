{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['Customer_ID', 'CustFName', 'CustLName', 'CustBirthDate', 'CustPhone']) }} as Customer_Key,
CUSTOMERID as Customer_ID,
CUSTOMERFNAME as CustFName,
CUSTOMERLNAME CustLName,
CUSTOMERBDAY as CustBirthDate,
CUSTOMERPHONE as CustPhone
FROM {{ source('samssubs_landing', 'customer') }}