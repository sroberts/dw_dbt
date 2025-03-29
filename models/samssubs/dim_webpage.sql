{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['PageURL']) }} as WebPage_Key,
Page_URL as PageURL
FROM {{ source('samssubs_landing_web', 'web_traffic_events') }}