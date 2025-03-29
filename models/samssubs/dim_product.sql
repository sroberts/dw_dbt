{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['ProductID', 'ProductType', 'ProductName', 'ProductCals', 'SandwichID', 'SandwichLength', 'SandwichBread']) }} as Product_Key,
dp.ProductID,
dp.ProductType,
dp.ProductName,
dp.PRODUCTCALORIES as ProductCals,
ds.SandwichID,
ds.SandwichLength,
ds.SandwichBread,
FROM {{ source('insurance_landing', 'product') }} as dp
JOIN {{ source('insurance_landing', 'sandwich') }} as ds WHERE ds.productid = dp.productid