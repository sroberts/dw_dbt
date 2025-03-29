{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['Event_Name']) }} as Event_Key,
Event_Name
FROM {{ source('samssubs_landing_web', 'web_traffic_events') }}