WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
)

SELECT
    CONCAT(coin_id, '_', CAST(price_date AS STRING)) AS price_id,
    coin_id,
    price_date,
    price_usd,
    market_cap,
    volume_24h,
    price_change_pct,
    extracted_at
FROM staging