INSERT INTO alertas_anomalias (
    category,
    sub_type,
    payload,
    created_at
)
SELECT
    'MARKET' AS category,
    'PRICE_VARIATION_20PCT' AS sub_type,
    jsonb_build_object(
        'market_id', o.market_id,
        'outcome_name', o.outcome_name,
        'current_price', o.current_price,
        'last_stable_price', m.last_stable_price,
        'variation_pct',
            ((o.current_price - m.last_stable_price)
            / NULLIF(m.last_stable_price, 0)) * 100,
        'abs_variation_pct',
            ABS((o.current_price - m.last_stable_price)
            / NULLIF(m.last_stable_price, 0)) * 100,
        'is_pre_flip',
            ABS((o.current_price - m.last_stable_price)
            / NULLIF(m.last_stable_price, 0)) * 100 > 50
    ) AS payload,
    NOW() AS created_at
FROM outcome_tokens o
JOIN mercados_master m
    ON o.market_id = m.market_id
WHERE
    m.last_stable_price IS NOT NULL
    AND m.last_stable_price <> 0
    AND ABS(
        (o.current_price - m.last_stable_price)
        / m.last_stable_price * 100
    ) > 20;