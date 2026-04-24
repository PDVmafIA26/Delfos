INSERT INTO alertas_anomalias (category, sub_type, payload, created_at)
SELECT
    'MARKET',
    'VOLUME_SPIKE_VS_HISTORY',
    jsonb_build_object(
        'ratio', v.volume_5min / m.historical_vol_5min_avg,
        'usd_detected', v.volume_5min
    ),
    now()
FROM ventanas_5min v
JOIN mercados_master m USING (market_id)
WHERE v.window_end > now() - interval '5 minutes'
AND (v.volume_5min / m.historical_vol_5min_avg) > 5;