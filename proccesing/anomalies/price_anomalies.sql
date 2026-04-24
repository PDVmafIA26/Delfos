WITH price_changes AS (
    SELECT
        market_id,
        outcome_name,
        current_price,
        updated_at,
        LAG(current_price) OVER (
            PARTITION BY market_id, outcome_name
            ORDER BY updated_at
        ) AS prev_price
    FROM outcome_tokens
)
INSERT INTO alertas_anomalias (
    category,
    sub_type,
    payload,
    created_at
)
SELECT
    'MARKET' AS category,
    'PRICE_CROSS_0_5' AS sub_type,
    jsonb_build_object(
        'market_id', market_id,
        'outcome_name', outcome_name,
        'previous_price', prev_price,
        'current_price', current_price,
        'change', CASE
            WHEN prev_price < 0.5 AND current_price >= 0.5 THEN 'NO_TO_YES'
            WHEN prev_price >= 0.5 AND current_price < 0.5 THEN 'YES_TO_NO'
        END
    ) AS payload,
    NOW() AS created_at
FROM price_changes
WHERE
    prev_price IS NOT NULL
    AND (
        (prev_price < 0.5 AND current_price >= 0.5)
        OR
        (prev_price >= 0.5 AND current_price < 0.5)
    );