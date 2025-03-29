{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['Date', 'Time', 'DayOfWeek', 'Month', 'Year']) }} as Date_Key
Date
Time
DayOfWeek
Month
Year
FROM {{ source('insurance_landing', 'agents') }} # FIX
CUSTOMERBDAY
EMPLOYEEBDAY
FROM {{ source('samssubs_landing_web', 'Web_Traffic_Events') }}
ORDER OrderDate