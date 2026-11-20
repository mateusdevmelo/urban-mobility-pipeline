from __future__ import annotations

from pathlib import Path


def test_sql_scripts_exist_and_not_empty():
    sql_dir = Path(__file__).resolve().parents[1] / "sql"
    assert sql_dir.exists()

    expected_files = [
        "01_create_schemas.sql",
        "02_reference_payment_types.sql",
        "03_create_silver_tables.sql",
        "04_create_gold_models.sql",
        "05_refresh_materialized_view.sql",
    ]

    for fname in expected_files:
        fpath = sql_dir / fname
        assert fpath.exists(), f"Arquivo SQL ausente: {fname}"
        content = fpath.read_text(encoding="utf-8").strip()
        assert len(content) > 10, f"Arquivo SQL vazio: {fname}"


def test_sql_schemas_contain_bronze_silver_gold():
    fpath = Path(__file__).resolve().parents[1] / "sql" / "01_create_schemas.sql"
    content = fpath.read_text(encoding="utf-8").lower()
    assert "create schema if not exists bronze" in content
    assert "create schema if not exists silver" in content
    assert "create schema if not exists gold" in content


def test_sql_payment_types_has_all_enums():
    fpath = Path(__file__).resolve().parents[1] / "sql" / "02_reference_payment_types.sql"
    content = fpath.read_text(encoding="utf-8")
    assert "Credit card" in content
    assert "Cash" in content
    assert "No charge" in content
    assert "Dispute" in content
    assert "Unknown" in content
    assert "Voided trip" in content


def test_sql_gold_materialized_view_defined():
    fpath = Path(__file__).resolve().parents[1] / "sql" / "04_create_gold_models.sql"
    content = fpath.read_text(encoding="utf-8").lower()
    assert "create materialized view gold.mv_monthly_taxi_metrics" in content
    assert "total_trips" in content
    assert "total_valid_revenue" in content
    assert "avg_ticket_per_trip" in content
    assert "avg_trip_distance" in content
