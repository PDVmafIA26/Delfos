/*
El flujo ideal sería: 
    1. Se ejecuta esta query cada X tiempo
    2. Se ejecuta update_price_token.sql
    3. Se limpia la tabla temporal (outcome_tokens_updates)
*/

WITH ordered_updates AS (
    SELECT
        asset_id,
        price,
        ts,
        LAG(price) OVER (
            PARTITION BY asset_id
            ORDER BY ts
        ) AS prev_price
    FROM outcome_tokens_updates
),
cross_events AS (
    SELECT
        asset_id,
        price,
        prev_price,
        ts,
        CASE
            WHEN prev_price < 0.5 AND price >= 0.5 THEN 'CROSS_UP'
            WHEN prev_price >= 0.5 AND price < 0.5 THEN 'CROSS_DOWN'
        END AS change
    FROM ordered_updates
    WHERE
        prev_price IS NOT NULL
        AND (
            (prev_price < 0.5 AND price >= 0.5)
            OR
            (prev_price >= 0.5 AND price < 0.5)
        )
),
deduplicated AS (
    SELECT DISTINCT ON (t.market_id, time_bucket)
        t.market_id,
        m.question_title,
        t.outcome_name,
        c.asset_id,
        c.price AS current_price,
        c.prev_price,
        to_timestamp(c.ts) AS event_timestamp,
        c.change
    FROM cross_events c
    JOIN outcome_tokens t
        ON t.id_token = c.asset_id
    JOIN mercados_master m
        ON m.market_id = t.market_id
    CROSS JOIN LATERAL (
        SELECT FLOOR(c.ts / 5) AS time_bucket /*Esto es por si cambia de SI a NO y luego un minuto más tarde cambia de NO a SI*/
    ) tb
    ORDER BY
        t.market_id,
        time_bucket,
        ABS(c.price - 0.5) DESC
)
INSERT INTO alertas_anomalias (
    category,
    sub_type,
    payload,
    created_at
)
SELECT
    'MARKET',
    'PRICE_CROSS_0_5',
    jsonb_build_object(
        'market_id', market_id,
        'question', question_title,
        'outcome_name', outcome_name,
        'previous_price', prev_price,
        'actual_price', current_price,
        'change', change,
        'event_timestamp', event_timestamp
    ),
    NOW()
FROM deduplicated;