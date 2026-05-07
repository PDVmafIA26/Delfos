WITH ordered_prices AS (
    SELECT
        l.asset_id,
        l.price,
        l.idautoincremental,
        LAG(l.price) OVER (
            PARTITION BY l.asset_id
            ORDER BY l.idautoincremental
        ) AS prev_price
    FROM last_trade_price l
),
crosses AS (
    SELECT
        asset_id,
        price AS current_price,
        prev_price,
        CASE
            WHEN prev_price < 0.5 AND price >= 0.5 THEN 'CROSS_UP'
            WHEN prev_price >= 0.5 AND price < 0.5 THEN 'CROSS_DOWN'
        END AS change
    FROM ordered_prices
    WHERE prev_price IS NOT NULL
    AND (
            (prev_price < 0.5 AND price >= 0.5)
        OR (prev_price >= 0.5 AND price < 0.5)
    )
)
INSERT INTO alertas_anomalias (
    category,
    sub_type,
    payload,
    created_at
)
SELECT
    'MARKET',
    'FLIP',
    jsonb_build_object(
        'market_id', t.condition_id,
        'question', m.question,
        'outcome_name', t.outcome_name,
        'previous_price', c.prev_price,
        'actual_price', c.current_price,
        'change', c.change
    ),
    NOW()
FROM crosses c
JOIN outcome_tokens t
    ON t.asset_id = c.asset_id
JOIN mercados_master m
    ON m.conditionid = t.condition_id;