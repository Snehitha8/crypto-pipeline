WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM staging
)

SELECT
    coin_id,
    symbol,
    name,
    ath,
    atl
FROM latest
WHERE row_num = 1