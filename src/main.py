from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from ingestion.downloader import TLCDownloader
from processing.spark_session import create_spark_session
from processing.silver_processor import SilverProcessor
from database.postgres_loader import PostgresLoader
from transform.dbt_runner import DbtRunner

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("urban_mobility_pipeline")


def run_pipeline(
    year: int = 2025,
    start_month: int = 1,
    end_month: int = 6,
    step: str = "all",
    force_download: bool = False,
) -> None:
    project_root = Path(__file__).resolve().parents[1]
    bronze_dir = Path(
        os.getenv("BRONZE_DIR", project_root / "data" / "bronze" / "yellow_taxi")
    )
    silver_dir = Path(
        os.getenv("SILVER_DIR", project_root / "data" / "silver" / "yellow_taxi")
    )
    manifest = project_root / "data" / "metadata" / "ingestion_manifest.json"

    loader = PostgresLoader()
    dbt_runner = DbtRunner(project_root)

    # 0. Inicialização do banco de dados (schemas e tabelas)
    if step in ("all", "init-db"):
        logger.info("=== [ETAPA 0] Inicializando Schemas e Tabelas no PostgreSQL ===")
        try:
            loader.init_database()
            logger.info("Estrutura do banco de dados inicializada com sucesso.")
        except Exception:
            logger.exception("Falha ao inicializar o banco de dados.")
            raise

        if step == "init-db":
            return

    # 1. Ingestão Bronze (Download dos arquivos Parquet da NYC TLC)
    downloaded_files = []

    if step in ("all", "bronze", "silver"):
        logger.info(
            "=== [ETAPA 1: BRONZE] Ingestão Incremental NYC TLC (%d-%02d até %d-%02d) ===",
            year,
            start_month,
            year,
            end_month,
        )

        downloader = TLCDownloader(bronze_dir, manifest)

        for month in range(start_month, end_month + 1):
            file_path = downloader.download_month(
                year, month, force=force_download
            )
            downloaded_files.append(file_path)

        logger.info(
            "Bronze concluída: %d arquivo(s) prontos.",
            len(downloaded_files),
        )

        if step == "bronze":
            return

    # 2. Processamento Silver (PySpark: regras de qualidade, anomalias, Parquet e PostgreSQL)
    if step in ("all", "silver"):
        logger.info(
            "=== [ETAPA 2: SILVER] Processamento PySpark & Carga PostgreSQL ==="
        )

        if not downloaded_files:
            # Localiza os arquivos caso o step bronze não tenha rodado nesta execução
            downloader = TLCDownloader(bronze_dir, manifest)

            for month in range(start_month, end_month + 1):
                p = (
                    bronze_dir
                    / f"year={year}"
                    / f"month={month:02d}"
                    / f"yellow_tripdata_{year}-{month:02d}.parquet"
                )

                if p.exists():
                    downloaded_files.append(p)
                else:
                    logger.info("Arquivo local ausente, baixando: %s", p.name)
                    downloaded_files.append(downloader.download_month(year, month))

        from pyspark.sql import functions as F

        spark = create_spark_session()

        try:
            processor = SilverProcessor(spark)
            bronze_df = processor.read_bronze(downloaded_files)

            # Tenta ler referência de pagamentos via JDBC; fallback em memória se indisponível
            payment_ref_df = loader.read_payment_reference(spark)
            silver_df = processor.transform(
                bronze_df,
                payment_ref_df=payment_ref_df,
            )

            # Filtra estritamente o período solicitado (YYYYMM)
            lo = f"{year}{start_month:02d}"
            hi = f"{year}{end_month:02d}"

            silver_df = silver_df.filter(
                F.col("pickup_year_month").between(lo, hi)
            )

            # Grava na camada Silver em Parquet particionado
            processor.write_silver_parquet(silver_df, silver_dir)

            # Carga atômica na tabela silver.yellow_taxi_trips por competência
            months = [
                row["pickup_year_month"]
                for row in silver_df.select("pickup_year_month").distinct().collect()
                if row["pickup_year_month"] is not None
            ]

            for ym in sorted(months):
                logger.info(
                    "Carregando partição silver %s no PostgreSQL...",
                    ym,
                )

                month_df = silver_df.filter(
                    silver_df.pickup_year_month == ym
                )

                loader.load_month(month_df, ym)

            logger.info(
                "Processamento da camada Silver concluído com sucesso."
            )

        finally:
            spark.stop()

        if step == "silver":
            return

    # 3. Modelagem Analítica Gold (dbt)
    if step in ("all", "gold"):
        logger.info(
            "=== [ETAPA 3: GOLD] Modelagem Analítica via dbt ==="
        )

        dbt_runner.build()

        try:
            metrics = dbt_runner.fetch_gold_metrics()

            logger.info(
                "--- Indicadores Analíticos da Camada Gold (Materialized View) ---"
            )

            for m in metrics:
                logger.info(
                    "Mês: %s | Vendor: %s (%s) | Corridas: %s | Rec. Válida: $%.2f | Ticket Médio: $%.2f | Dist. Média: %.2f mi",
                    m.get("pickup_year_month"),
                    m.get("vendor_id"),
                    m.get("vendor_name"),
                    f"{m.get('total_trips', 0):,}",
                    float(m.get("total_valid_revenue") or 0.0),
                    float(m.get("avg_ticket_per_trip") or 0.0),
                    float(m.get("avg_trip_distance") or 0.0),
                )

        except Exception as e:
            logger.warning(
                "Não foi possível consultar as métricas da Materialized View: %s",
                e,
            )

        logger.info(
            "=== Pipeline Urban Mobility concluído com sucesso! ==="
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Urban Mobility Analytics — Pipeline Medalhão"
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Ano dos dados TLC (padrão: 2025)",
    )

    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
        help="Mês inicial (1-12, padrão: 1)",
    )

    parser.add_argument(
        "--end-month",
        type=int,
        default=6,
        help="Mês final (1-12, padrão: 6)",
    )

    parser.add_argument(
        "--step",
        type=str,
        choices=["all", "init-db", "bronze", "silver", "gold"],
        default="all",
        help="Executa etapa específica ou todo o pipeline",
    )

    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Força novo download mesmo se no manifesto",
    )

    args = parser.parse_args()

    run_pipeline(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        step=args.step,
        force_download=args.force_download,
    )


if __name__ == "__main__":
    main()
