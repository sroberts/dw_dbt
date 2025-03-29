{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['EmployeeID', 'EmpFName','EmployeeLName', 'EmpBirthDate']) }} as Employee_Key, 
EmployeeID,
EmpFName,
EmpLName,
EMPLOYEEBDAY as EmpBirthDate
FROM {{ source('samssubs_landing', 'employee') }} 