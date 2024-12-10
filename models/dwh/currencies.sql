WITH staged_data AS (
    SELECT
        id AS currency_code, -- Map `id` to `CURRENCY_CODE`
        name AS currency_name, -- Map `name` to `CURRENCY_NAME`
        symbol AS currency_symbol, -- Map `symbol` to `CURRENCY_SYMBOL`
        CURRENT_TIMESTAMP AS timestamp,
        md5(concat_ws(
            '|',
            id,  -- Assuming `id` is a unique identifier in the source data
            name,
            symbol
        )) AS currency_id -- Generate `CURRENCY_ID` using hash
    FROM {{ source('staging', 'stg_currencies') }}
),
existing_data AS (
    SELECT
        currency_code,
        currency_name,
        currency_symbol,
        currency_id,
        timestamp
    FROM {{ source('dwh', 'currencies') }}
),
final AS (
    SELECT
        staged_data.currency_code,
        staged_data.currency_name,
        staged_data.currency_symbol,
        staged_data.currency_id,
        staged_data.timestamp
    FROM staged_data
    LEFT JOIN existing_data
    ON staged_data.currency_code = existing_data.currency_code
    WHERE existing_data.currency_id IS DISTINCT FROM staged_data.currency_id
)
SELECT
    currency_id, -- Use hash-based unique identifier
    currency_code,
    currency_name,
    currency_symbol,
    timestamp
FROM final
