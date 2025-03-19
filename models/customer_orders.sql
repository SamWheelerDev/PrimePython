{{ config(
    materialized='table',
    schema='analytics',
    tags=['daily', 'customers']
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

order_payments AS (
    SELECT
        order_id,
        SUM(amount) AS total_amount
    FROM payments
    GROUP BY order_id
),

customer_orders AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        MIN(orders.order_date) AS first_order_date,
        MAX(orders.order_date) AS most_recent_order_date,
        COUNT(DISTINCT orders.order_id) AS number_of_orders,
        SUM(order_payments.total_amount) AS lifetime_value
    FROM customers
    LEFT JOIN orders ON customers.customer_id = orders.customer_id
    LEFT JOIN order_payments ON orders.order_id = order_payments.order_id
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM customer_orders