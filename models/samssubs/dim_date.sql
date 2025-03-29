{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
    cast(EVENT_TIMESTAMP as date) as Date,
    cast(EVENT_TIMESTAMP as time) as Time,
    extract(dow from cast(EVENT_TIMESTAMP as date)) as DayOfWeek,
    extract(month from cast(EVENT_TIMESTAMP as date)) as Month,
    extract(year from cast(EVENT_TIMESTAMP as date)) as Year
FROM {{ source('samssubs_landing', 'customer') }}

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
cast(agent_date as date) as Date,
cast('00:00:00' as time) as Time,
extract(dow from cast(agent_date as date)) as DayOfWeek,
extract(month from cast(agent_date as date)) as Month,
extract(year from cast(agent_date as date)) as Year
FROM {{ source('samssubs_landing', 'customer') }} CUSTOMERBDAY

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
cast(agent_date as date) as Date,
cast('00:00:00' as time) as Time,
extract(dow from cast(agent_date as date)) as DayOfWeek,
extract(month from cast(agent_date as date)) as Month,
extract(year from cast(agent_date as date)) as Year
FROM {{ source('samssubs_landing', 'employee') }} EMPLOYEEBDAY

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
cast(agent_date as date) as Date,
cast('00:00:00' as time) as Time,
extract(dow from cast(agent_date as date)) as DayOfWeek,
extract(month from cast(agent_date as date)) as Month,
extract(year from cast(agent_date as date)) as Year
FROM {{ source('samssubs_landing_web', 'web_traffic_events') }}

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
cast(agent_date as date) as Date,
cast('00:00:00' as time) as Time,
extract(dow from cast(agent_date as date)) as DayOfWeek,
extract(month from cast(agent_date as date)) as Month,
extract(year from cast(agent_date as date)) as Year
FROM {{ source('samssubs_landing', 'order') }} ORDER OrderDate