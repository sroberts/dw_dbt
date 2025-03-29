{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['TrafficSource']) }} as TrafficSource_Key,
TrafficSource
FROM {{ source('samssubs_landing_web', 'Web_Traffic_Events') }}