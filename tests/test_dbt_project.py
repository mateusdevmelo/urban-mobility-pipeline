from __future__ import annotations

from pathlib import Path


def test_dbt_directory_structure():
    dbt_dir = Path(__file__).resolve().parents[1] / "dbt"
    assert (dbt_dir / "dbt_project.yml").exists()
    assert (dbt_dir / "profiles.yml").exists()

    staging_models = dbt_dir / "models" / "staging"
    assert (staging_models / "stg_yellow_taxi_trips.sql").exists()
    assert (staging_models / "stg_payment_types.sql").exists()

    gold_models = dbt_dir / "models" / "gold"
    assert (gold_models / "dim_vendor.sql").exists()
    assert (gold_models / "dim_payment_type.sql").exists()
    assert (gold_models / "dim_date.sql").exists()
    assert (gold_models / "fct_taxi_trips.sql").exists()
    assert (gold_models / "mv_monthly_taxi_metrics.sql").exists()


def test_dbt_gold_mv_metrics():
    mv_path = Path(__file__).resolve().parents[1] / "dbt" / "models" / "gold" / "mv_monthly_taxi_metrics.sql"
    content = mv_path.read_text(encoding="utf-8").lower()

    # Requisito 7: indicadores por mês, vendorid, mês (yyyymm) e valor:
    assert "pickup_year_month" in content
    assert "vendor_id" in content
    assert "total_trips" in content
    assert "total_valid_revenue" in content
    assert "avg_ticket_per_trip" in content
    assert "avg_trip_distance" in content
