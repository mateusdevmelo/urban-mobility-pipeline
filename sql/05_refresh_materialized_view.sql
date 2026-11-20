-- 05_refresh_materialized_view.sql
-- Atualiza a visão materializada com os novos dados processados

REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_monthly_taxi_metrics;
