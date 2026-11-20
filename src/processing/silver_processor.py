from __future__ import annotations

import logging
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

logger = logging.getLogger(__name__)


class SilverProcessor:
    """Lê Parquet Bronze, aplica regras de qualidade/anomalia e gera Parquet Silver particionado."""

    REQUIRED_SOURCE_COLUMNS = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "payment_type",
        "fare_amount",
        "total_amount",
    ]

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_bronze(self, files: list[str | Path]) -> DataFrame:
        """Lê múltiplos arquivos Parquet da camada bronze e valida colunas mandatórias."""
        if not files:
            raise ValueError("Nenhum arquivo Bronze foi informado para leitura.")

        file_paths = [str(path) for path in files]
        logger.info("Lendo %d arquivo(s) Parquet da camada Bronze...", len(file_paths))
        df = self.spark.read.parquet(*file_paths)

        missing = sorted(set(self.REQUIRED_SOURCE_COLUMNS) - set(df.columns))
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes nos arquivos Parquet: {missing}")

        return df

    def get_default_payment_reference(self) -> DataFrame:
        """Retorna o DataFrame de referência oficial dos tipos de pagamento segundo NYC TLC."""
        return self.spark.createDataFrame(
            [
                (1, "Credit card", True),
                (2, "Cash", True),
                (3, "No charge", False),
                (4, "Dispute", False),
                (5, "Unknown", False),
                (6, "Voided trip", False),
            ],
            ["payment_type", "payment_description", "is_valid_payment"],
        )

    def transform(self, df: DataFrame, payment_ref_df: DataFrame | None = None) -> DataFrame:
        """
        Aplica regras de negócio, qualidade e sinalização de anomalias:
        1. Padronização de tipos de dados (casts)
        2. Cálculo de duração da viagem e competência mensal (pickup_year_month)
        3. Enriquecimento com tipos de pagamento válidos/inválidos
        4. Avaliação de 6 regras de qualidade estruturais (is_valid_trip, invalid_reason)
        5. Sinalização de anomalias (>6 horas de duração ou >100 milhas)
        6. Cálculo de receita válida (valid_revenue_amount)
        7. Geração de chave primária determinística e deduplicação (trip_id)
        """
        # 1. Padronização e conversão explícita de tipos
        df = (
            df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
              .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
              .withColumn("passenger_count", F.col("passenger_count").cast(DoubleType()))
              .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
              .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
              .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
              .withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
              .withColumn("dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        )

        # 2. Criação de colunas temporais e duração em minutos
        df = (
            df.withColumn(
                "trip_duration_minutes",
                (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0,
            )
            .withColumn("pickup_date", F.to_date("pickup_datetime"))
            .withColumn("pickup_year_month", F.date_format("pickup_datetime", "yyyyMM"))
        )

        # 3. Enriquecimento com referência de pagamentos
        if payment_ref_df is None:
            payment_ref_df = self.get_default_payment_reference()

        df = df.join(F.broadcast(payment_ref_df), on="payment_type", how="left")
        df = df.withColumn(
            "is_valid_payment",
            F.coalesce(F.col("is_valid_payment"), F.lit(False)),
        ).withColumn(
            "payment_description",
            F.coalesce(F.col("payment_description"), F.lit("Unknown")),
        )

        # 4. Regras de Qualidade de Dados (6 regras estruturais)
        reasons = [
            (F.col("pickup_datetime").isNull(), "pickup_datetime_nula"),
            (F.col("dropoff_datetime").isNull(), "dropoff_datetime_nula"),
            (F.col("dropoff_datetime") <= F.col("pickup_datetime"), "desembarque_antes_ou_igual_embarque"),
            (F.col("trip_duration_minutes") <= 0, "duracao_nao_positiva"),
            (F.col("trip_distance").isNull() | (F.col("trip_distance") < 0), "distancia_nula_ou_negativa"),
            (F.col("total_amount").isNull() | (F.col("total_amount") < 0), "valor_total_nulo_ou_negativo"),
        ]

        reason_array = F.array(*[
            F.when(condition, F.lit(reason))
            for condition, reason in reasons
        ])

        filtered_reasons = F.filter(reason_array, lambda x: x.isNotNull())

        df = df.withColumn(
            "invalid_reason",
            F.when(F.size(filtered_reasons) > 0, F.concat_ws(";", filtered_reasons)).otherwise(F.lit(None)),
        ).withColumn(
            "is_valid_trip",
            F.size(filtered_reasons) == 0,
        )

        # 5. Detecção de Anomalias (não invalidam a corrida por si sós, mas sinalizam auditoria)
        is_long_duration = F.col("trip_duration_minutes") > 360.0
        is_long_distance = F.col("trip_distance") > 100.0

        anomaly_array = F.array(
            F.when(is_long_duration, F.lit("duracao_acima_6_horas")),
            F.when(is_long_distance, F.lit("distancia_acima_100_milhas")),
        )
        filtered_anomalies = F.filter(anomaly_array, lambda x: x.isNotNull())

        df = (
            df.withColumn(
                "is_anomaly",
                is_long_duration | is_long_distance,
            )
            .withColumn(
                "anomaly_reason",
                F.when(F.size(filtered_anomalies) > 0, F.concat_ws(";", filtered_anomalies)).otherwise(F.lit(None)),
            )
            # 6. Considerar como receita válida apenas viagens válidas E com pagamento válido
            .withColumn(
                "valid_revenue_amount",
                F.when(
                    F.col("is_valid_trip") & F.col("is_valid_payment"),
                    F.col("total_amount"),
                ).otherwise(F.lit(0.0)),
            )
        )

        # 7. Identificador determinístico da corrida (SHA-256) e deduplicação
        id_columns = [
            F.coalesce(F.col("VendorID").cast("string"), F.lit("")),
            F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("PULocationID").cast("string"), F.lit("")) if "PULocationID" in df.columns else F.lit(""),
            F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")) if "DOLocationID" in df.columns else F.lit(""),
            F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
        ]
        df = df.withColumn("trip_id", F.sha2(F.concat_ws("|", *id_columns), 256))
        df = df.dropDuplicates(["trip_id"])

        return df

    def write_silver_parquet(self, df: DataFrame, output_dir: str | Path) -> None:
        """Grava Parquet particionado dinamicamente por pickup_year_month na Silver."""
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        logger.info("Gravando dados Silver em Parquet particionado em: %s", out_path)
        (
            df.write
            .mode("overwrite")
            .partitionBy("pickup_year_month")
            .parquet(str(out_path))
        )
        logger.info("Gravação de Parquet Silver concluída.")
