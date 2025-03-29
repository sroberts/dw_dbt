{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['OrderMethod']) }} as OrderMethod_Key,
OrderMethod
FROM {{ source('insurance_landing', 'order') }}