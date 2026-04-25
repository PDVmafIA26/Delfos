/*
Se queda con el último precio por asset y
hace update en bloque (mucho más rápido)
*/
UPDATE outcome_tokens t
SET current_price = u.price
FROM (
    SELECT DISTINCT ON (asset_id)
        asset_id, price
    FROM outcome_tokens_updates
    ORDER BY asset_id, ts DESC
) u
WHERE t.id_token = u.asset_id;