{{ config(
    materialized='view'
) }}

WITH daily_avg_exchange_rates AS (
    SELECT
        base_currency,
        target_currency,
        DATE_TRUNC('day', timestamp) AS date,
        AVG(exchange_rate) AS avg_exchange_rate
    FROM {{ source('dwh', 'exchange_rates') }}
    GROUP BY base_currency, target_currency, DATE_TRUNC('day', timestamp)
),
rate_trends AS (
    SELECT
        base_currency,
        target_currency,
        date,
        avg_exchange_rate,
        LAG(avg_exchange_rate) OVER (
            PARTITION BY base_currency, target_currency
            ORDER BY date
        ) AS previous_avg_exchange_rate,
        CASE
            WHEN LAG(avg_exchange_rate) OVER (
                PARTITION BY base_currency, target_currency
                ORDER BY date
            ) IS NULL THEN NULL
            ELSE avg_exchange_rate - LAG(avg_exchange_rate) OVER (
                PARTITION BY base_currency, target_currency
                ORDER BY date
            )
        END AS rate_change
    FROM daily_avg_exchange_rates
)
SELECT
    base_currency,
    target_currency,
    date,
    avg_exchange_rate,
    rate_change
FROM rate_trends
ORDER BY date, base_currency, target_currency
