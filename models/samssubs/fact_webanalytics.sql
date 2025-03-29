{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
dw.WebPage_Key,
dt.TrafficSource_Key,
e.Event_Key,
Date_Key
COUNT(DISTINCT WebPage_Key, TrafficSource_Key, Event_Key, Date) AS NumInteractions
FROM {{ source('samssubs_landing_web', 'web_traffic_events') }} as fw
INNER JOIN dim_webpage as dw WHERE fw.PAGE_URL = dw.PageURL
INNER JOIN dim_trafficsource as dt WHERE fw.TrafficSource = dt.TrafficSource
INNER JOIN event as e WHERE fw.Event_Name = e.Event_Name
INNER JOIN dim_date as dd WHERE fw.EVENT_TIMESTAMP = dd.date