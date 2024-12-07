WITH staged_data AS (
    SELECT
        BASE AS base_currency, -- Map `BASE` to `base_currency`
        CURRENCY AS target_currency, -- Map `CURRENCY` to `target_currency`
        RATE AS exchange_rate, -- Map `RATE` to `exchange_rate`
        CURRENT_TIMESTAMP AS timestamp,
        md5(concat_ws(
            '|',
            BASE,
            CURRENCY,
            RATE
        )) AS rate_id -- Generate a unique identifier for the record
    FROM {{ source('staging', 'stg_exchange_rates') }}
),
existing_data AS (
    SELECT
        rate_id
    FROM {{ source('dwh', 'exchange_rates') }} -- Reference the existing data
),
new_data AS (
    SELECT
        staged_data.rate_id,
        staged_data.base_currency,
        staged_data.target_currency,
        staged_data.exchange_rate,
        staged_data.timestamp
    FROM staged_data
    LEFT JOIN existing_data
    ON staged_data.rate_id = existing_data.rate_id
    WHERE existing_data.rate_id IS NULL -- Only keep new records
)
SELECT
    rate_id,
    base_currency,
    target_currency,
    exchange_rate,
    timestamp
FROM new_data
WHERE exchange_rate IS NOT NULL