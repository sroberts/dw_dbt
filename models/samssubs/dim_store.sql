{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['StoreID', 'StoreAddress', 'StoreCity', 'StoreState', 'StoreZip']) }} as Store_Key,
StoreID
ADDRESS as StoreAddress,
CITY as StoreCity,
STATE as StoreState,
ZIP as StoreZip
FROM {{ source('insurance_landing', 'agents') }} # FIX