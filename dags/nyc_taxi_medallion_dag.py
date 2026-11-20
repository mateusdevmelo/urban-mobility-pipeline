
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv


# ============================================================
# CONFIGURAÇÕES INICIAIS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

# Carrega as variáveis de ambiente do projeto
load_dotenv(PROJECT_ROOT / ".env")

# Disponibiliza os módulos Python do pipeline
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "urban_mobility_analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# TASK 1 - BRONZE
# ============================================================

def download_bronze_callable(**context) -> list[str]:
    """
    Task 1: Baixa arquivos Parquet da NYC TLC
    e grava na camada Bronze.
    """

    from ingestion.downloader import TLCDownloader

    dag_run = context.get("dag_run")
    dag_run_conf = (dag_run.conf or {}) if dag_run else {}

    year = int(dag_run_conf.get("year", 2025))
    start_month = int(dag_run_conf.get("start_month", 1))
    end_month = int(dag_run_conf.get("end_month", 6))
    force = bool(dag_run_conf.get("force_download", False))

    if not 1 <= start_month <= 12 or not 1 <= end_month <= 12:
        raise ValueError("Os meses devem estar entre 1 e 12.")

    if start_month > end_month:
        raise ValueError("start_month não pode ser maior que end_month.")

    bronze_dir = Path(
        os.getenv(
            "BRONZE_DIR",
            str(PROJECT_ROOT / "data" / "bronze" / "yellow_taxi"),
        )
    )

    manifest = (
        PROJECT_ROOT
        / "data"
        / "metadata"
        / "ingestion_manifest.json"
    )

    logger.info(
        "Iniciando download Bronze: ano=%s, meses=%s a %s",
        year,
        start_month,
        end_month,
    )

    downloader = TLCDownloader(bronze_dir, manifest)

    downloaded_paths = []

    for month in range(start_month, end_month + 1):
        file_path = downloader.download_month(
            year,
            month,
            force=force,
        )

        downloaded_paths.append(str(file_path))

    logger.info(
        "Download Bronze concluído. Arquivos: %s",
        downloaded_paths,
    )

    return downloaded_paths


# ============================================================
# TASK 2 - SILVER
# ============================================================



def process_silver_callable(**context) -> None:
    """
    Task 2: Processa cada competência individualmente,
    grava Parquet na Silver e carrega os dados no PostgreSQL.
    """

    import shutil

    from processing.spark_session import create_spark_session
    from processing.silver_processor import SilverProcessor
    from database.postgres_loader import PostgresLoader
    from pyspark.sql import functions as F

    dag_run = context.get("dag_run")
    dag_run_conf = (dag_run.conf or {}) if dag_run else {}

    year = int(dag_run_conf.get("year", 2025))
    start_month = int(dag_run_conf.get("start_month", 1))
    end_month = int(dag_run_conf.get("end_month", 6))

    if not 1 <= start_month <= 12 or not 1 <= end_month <= 12:
        raise ValueError("Os meses devem estar entre 1 e 12.")

    if start_month > end_month:
        raise ValueError("start_month não pode ser maior que end_month.")

    bronze_dir = Path(
        os.getenv(
            "BRONZE_DIR",
            str(PROJECT_ROOT / "data" / "bronze" / "yellow_taxi"),
        )
    )

    silver_dir = Path(
        os.getenv(
            "SILVER_DIR",
            str(PROJECT_ROOT / "data" / "silver" / "yellow_taxi"),
        )
    )

    ti = context.get("ti")

    files = (
        ti.xcom_pull(task_ids="task_download_bronze")
        if ti
        else None
    )

    if not files:
        files = [
            str(
                bronze_dir
                / f"year={year}"
                / f"month={month:02d}"
                / f"yellow_tripdata_{year}-{month:02d}.parquet"
            )
            for month in range(start_month, end_month + 1)
        ]

    files_by_month = {}

    for file in files:
        file_path = Path(file)
        file_month = int(file_path.name.split("-")[-1].split(".")[0])
        files_by_month[file_month] = file

    spark = create_spark_session()
    loader = PostgresLoader()
    processor = SilverProcessor(spark)

    try:
        payment_ref_df = loader.read_payment_reference(spark)

        for month in range(start_month, end_month + 1):

            if month not in files_by_month:
                raise FileNotFoundError(
                    f"Arquivo Bronze não encontrado para {year}-{month:02d}"
                )

            year_month = f"{year}{month:02d}"
            bronze_file = files_by_month[month]

            logger.info(
                "Iniciando processamento Silver da competência %s",
                year_month,
            )

            # 1. Leitura Bronze
            bronze_df = processor.read_bronze([bronze_file])

            # 2. Transformação Silver
            silver_df = processor.transform(
                bronze_df,
                payment_ref_df=payment_ref_df,
            )

            silver_df = silver_df.filter(
                F.col("pickup_year_month") == year_month
            )

            # 3. Diretórios temporário, oficial e backup
            temp_dir = silver_dir.parent / f".silver_processing_{year_month}"
            final_partition = silver_dir / f"pickup_year_month={year_month}"
            backup_dir = silver_dir.parent / f".silver_backup_{year_month}"

            if temp_dir.exists():
                shutil.rmtree(temp_dir)

            if backup_dir.exists():
                shutil.rmtree(backup_dir)

            # 4. Gravação temporária
            processor.write_silver_parquet(silver_df, temp_dir)

            # 5. Validação da gravação
            temp_partition = temp_dir / f"pickup_year_month={year_month}"

            validated_df = spark.read.parquet(str(temp_partition))
            row_count = validated_df.count()

            if row_count == 0:
                raise RuntimeError(
                    f"Nenhum registro Silver encontrado para {year_month}"
                )

            logger.info(
                "Validação concluída para %s: %s registros.",
                year_month,
                row_count,
            )

            # 6. Preserva a partição anterior, se existir
            if final_partition.exists():
                shutil.move(str(final_partition), str(backup_dir))

            try:
                # 7. Publica a nova partição Silver
                shutil.move(
                    str(temp_partition),
                    str(final_partition),
                )

                # 8. Lê os arquivos já publicados.
                # A coluna de partição é adicionada explicitamente,
                # pois estamos lendo diretamente o diretório da partição.
                published_df = (
                    spark.read
                    .parquet(str(final_partition))
                    .withColumn(
                        "pickup_year_month",
                        F.lit(year_month),
                    )
                )

                # 9. Carga idempotente no PostgreSQL
                loader.load_month(published_df, year_month)

            except Exception:
                logger.exception(
                    "Falha ao publicar ou carregar a competência %s. "
                    "Tentando restaurar a partição anterior.",
                    year_month,
                )

                # Remove a nova partição incompleta ou não confirmada
                if final_partition.exists():
                    shutil.rmtree(final_partition)

                # Restaura a versão anterior
                if backup_dir.exists():
                    shutil.move(
                        str(backup_dir),
                        str(final_partition),
                    )

                raise

            # 10. Remove backup somente após a carga bem-sucedida
            if backup_dir.exists():
                shutil.rmtree(backup_dir)

            if temp_dir.exists():
                shutil.rmtree(temp_dir)

            logger.info(
                "Competência %s concluída com sucesso: %s registros.",
                year_month,
                row_count,
            )

        logger.info("Camada Silver concluída para todo o período.")

    finally:
        spark.stop()



# ============================================================
# TASK 3 - GOLD
# ============================================================

def process_gold_callable(**context) -> None:
    """
    Task 3: Executa a modelagem analítica da camada Gold
    exclusivamente através do dbt.
    """

    from transform.dbt_runner import DbtRunner

    logger.info(
        "Iniciando transformação da camada Gold via dbt..."
    )

    runner = DbtRunner(PROJECT_ROOT)

    # Execução exclusiva do dbt
    runner.build()

    logger.info(
        "dbt build concluído. Consultando métricas Gold..."
    )

    try:
        metrics = runner.fetch_gold_metrics()

        logger.info(
            "--- Métricas Consolidadas da Gold ---"
        )

        for m in metrics:
            logger.info(
                "Mês: %s | Vendor: %s | Corridas: %s | "
                "Receita Válida: $%.2f | Ticket Médio: $%.2f | "
                "Distância Média: %.2f mi",
                m.get("pickup_year_month"),
                m.get("vendor_id"),
                f"{m.get('total_trips', 0):,}",
                float(m.get("total_valid_revenue") or 0.0),
                float(m.get("avg_ticket_per_trip") or 0.0),
                float(m.get("avg_trip_distance") or 0.0),
            )

    except Exception as e:
        logger.warning(
            "Não foi possível consultar as métricas Gold: %s",
            e,
        )

    logger.info(
        "Camada Gold concluída com sucesso."
    )


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

with DAG(
    dag_id="urban_mobility_nyc_taxi_pipeline",
    default_args=default_args,
    description=(
        "Pipeline medalhão semanal de corridas NYC TLC "
        "(Bronze -> Silver -> Gold)"
    ),
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=[
        "urban_mobility",
        "tlc",
        "medallion",
        "pyspark",
        "dbt",
        "gold_mv",
    ],
) as dag:

    task_download_bronze = PythonOperator(
        task_id="task_download_bronze",
        python_callable=download_bronze_callable,
    )

    task_process_silver = PythonOperator(
        task_id="task_process_silver",
        python_callable=process_silver_callable,
    )

    task_process_gold = PythonOperator(
        task_id="task_process_gold",
        python_callable=process_gold_callable,
    )

    # Fluxo de execução do pipeline medalhão
    (
        task_download_bronze
        >> task_process_silver
        >> task_process_gold
    )
