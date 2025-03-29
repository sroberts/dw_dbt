{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['Event_Name']) }} as Event_Key,


FROM {{ source('samssubs_landing_web', 'Web_Traffic_Events') }}