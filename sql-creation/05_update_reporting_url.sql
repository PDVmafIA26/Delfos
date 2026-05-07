-- =============================================================================
-- PROYECTO DELFOS — CAPA ORO
-- 05_update_reporting_url.sql
--
-- Actualiza la URL de reportes en la tabla config para apuntar
-- al servicio real de notificaciones (gremio de Reportes).
--
-- EJECUTAR cuando el gremio de notificaciones confirme su URL definitiva.
-- =============================================================================

-- ─── Ver configuración actual ────────────────────────────────────────────────
SELECT key, value FROM config ORDER BY key;

-- ─── Actualizar URL al servicio real de notificaciones ───────────────────────
-- El endpoint real del gremio de Reportes/Notificaciones es POST /notify
-- NOTA: el mock_reporting_server.py local usa /anomaly (solo para pruebas)
UPDATE config
SET value      = 'http://notifications:8000/notify',
    updated_at = NOW()
WHERE key = 'reporting_url';

-- Verificar cambio
SELECT key, value, updated_at FROM config WHERE key = 'reporting_url';


-- =============================================================================
-- ALINEACIÓN DE TIPOS: config.py del gremio de Reportes vs nuestros sub_type
--
-- El config.py del gremio usa:
--   DEFAULT_IMAGES = {
--       "FLIP":         "assets/default_images/flip.jpg",
--       "SUSPECT_USER": "assets/default_images/suspect.png",
--   }
--
-- Nuestros sub_type en la tabla anomalias son:
--   FLIP, SPIKE, PRICE_VAR, WHALE_MOVE, FLASH_ACC
--
-- Conclusión: "SUSPECT_USER" cubre lo que nosotros llamamos WHALE_MOVE y FLASH_ACC.
-- Opciones:
--   A) El gremio de Reportes añade imágenes para SPIKE y PRICE_VAR también.
--   B) Nosotros mapeamos WHALE_MOVE → SUSPECT_USER en el payload.
--
-- Por ahora, añadimos el campo image_key al payload para que Reportes lo use:
-- =============================================================================

-- Vista auxiliar que añade el image_key esperado por el config.py de Reportes
CREATE OR REPLACE VIEW v_anomalias_con_imagen AS
SELECT
    a.alert_id,
    a.category,
    a.sub_type,
    -- image_key: mapeo hacia los nombres del config.py de Reportes
    CASE a.sub_type
        WHEN 'FLIP'       THEN 'FLIP'
        WHEN 'SPIKE'      THEN 'FLIP'         -- reutilizamos flip.jpg hasta que añadan spike.jpg
        WHEN 'PRICE_VAR'  THEN 'FLIP'
        WHEN 'WHALE_MOVE' THEN 'SUSPECT_USER'
        WHEN 'FLASH_ACC'  THEN 'SUSPECT_USER'
    END AS image_key,
    a.payload,
    a.created_at
FROM anomalias a
ORDER BY a.created_at DESC;

COMMENT ON VIEW v_anomalias_con_imagen IS
  'Anomalías con image_key alineado al DEFAULT_IMAGES del config.py del gremio de Reportes.';
