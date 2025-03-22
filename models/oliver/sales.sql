{{ config(
    materialized = 'table',
    schema = 'semantic_layer'
)}}

WITH sales_data AS (
    SELECT
        s.sales_id,
        s.sale_date,
        s.amount,
        s.product_id,
        s.customer_id,
        s.agent_id,
        d.date_day,
        d.day_of_week,
        d.month_name,
        d.quarter_of_year,
        d.year_number,
        p.product_name,
        p.category,
        p.unit_price,
        c.customer_name,
        c.customer_segment,
        c.region,
        a.firstname AS agent_first_name,
        a.lastname AS agent_last_name,
        a.email AS agent_email
    FROM {{ ref('fact_sales') }} s
    LEFT JOIN {{ ref('oliver_dim_date') }} d
        ON s.sale_date = d.date_day
    LEFT JOIN {{ ref('oliver_dim_product') }} p
        ON s.product_id = p.product_key
    LEFT JOIN {{ ref('oliver_dim_customer') }} c
        ON s.customer_id = c.customer_key
),

-- Add calculated metrics
sales_metrics AS (
    SELECT
        sales_data.*,
        s.amount / p.unit_price AS quantity_sold,
        CASE 
            WHEN c.customer_segment = 'Premium' THEN s.amount * 0.15
            WHEN c.customer_segment = 'Standard' THEN s.amount * 0.10
            ELSE s.amount * 0.05
        END AS estimated_profit
    FROM sales_data
)

SELECT
    -- Time dimensions
    date_day,
    day_of_week,
    month_name,
    quarter_of_year,
    year_number,
    
    -- Product dimensions
    product_id,
    product_name,
    category,
    
    -- Customer dimensions
    customer_id,
    customer_name,
    customer_segment,
    region,
    
    -- Agent dimensions
    agent_id,
    agent_first_name || ' ' || agent_last_name AS agent_name,
    agent_email,
    
    -- Sales metrics
    sales_id,
    amount AS sales_amount,
    quantity_sold,
    unit_price,
    estimated_profit,
    
    -- Additional calculated metrics
    SUM(amount) OVER (PARTITION BY year_number, month_name) AS monthly_sales,
    SUM(amount) OVER (PARTITION BY year_number, quarter_of_year) AS quarterly_sales,
    SUM(amount) OVER (PARTITION BY agent_id) AS agent_total_sales,
    SUM(amount) OVER (PARTITION BY customer_id) AS customer_lifetime_value,
    RANK() OVER (PARTITION BY year_number, month_name ORDER BY amount DESC) AS sale_rank_in_month
FROM sales_metrics