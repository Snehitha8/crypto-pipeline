WITH source AS (
    SELECT
        id AS coin_id,
        symbol,
        name,
        current_price AS price_usd,
        market_cap,
        total_volume AS volume_24h,
        price_change_percentage_24h AS price_change_pct,
        ath,
        atl,
        DATE(extracted_at) AS price_date,
        extracted_at
    FROM {{ source('crypto_raw', 'raw_prices') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, price_date
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM source
)

SELECT * EXCEPT (row_num)
FROM deduplicated
WHERE row_num = 1