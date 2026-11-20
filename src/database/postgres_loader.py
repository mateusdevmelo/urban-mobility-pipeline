from __future__ import annotations

import logging
import os
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Gerencia a conexão com o PostgreSQL, execução de DDLs e carga atômica da Silver."""

    def __init__(self):
        self.host = os.environ.get("POSTGRES_HOST", "localhost")
        self.port = os.environ.get("POSTGRES_PORT", "5433")
        self.database = os.environ.get("POSTGRES_DB", "urban_mobility_db")
        self.user = os.environ.get("POSTGRES_USER", "postgres")
        self.password = os.environ.get("POSTGRES_PASSWORD")

        if not self.password:
            raise ValueError(
            "A variável POSTGRES_PASSWORD não foi configurada."
    )
        self.jdbc_url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        )
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    def get_connection(self):
        import psycopg2
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

    def execute_sql_file(self, sql_path: str | Path) -> None:
        """Executa um arquivo SQL arbitrário no PostgreSQL."""
        path = Path(sql_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {path}")

        logger.info("Executando script SQL: %s", path.name)
        sql_content = path.read_text(encoding="utf-8")
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_content)
            conn.commit()
        logger.info("Script %s executado com sucesso.", path.name)

    def init_database(self, sql_dir: str | Path | None = None) -> None:
        """Executa scripts iniciais de criação de schemas, tabelas e dados de referência."""
        base_dir = Path(sql_dir) if sql_dir else Path(__file__).resolve().parents[2] / "sql"
        scripts = [
            "01_create_schemas.sql",
            "02_reference_payment_types.sql",
            "03_create_silver_tables.sql",
        ]
        for script in scripts:
            script_path = base_dir / script
            if script_path.exists():
                self.execute_sql_file(script_path)

    def read_payment_reference(self, spark: SparkSession) -> DataFrame | None:
        """Lê a tabela oficial silver.ref_payment_type via JDBC no PostgreSQL."""
        try:
            logger.info("Lendo tabela silver.ref_payment_type via JDBC...")
            df = (
                spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", "silver.ref_payment_type")
                .option("user", self.user)
                .option("password", self.password)
                .option("driver", "org.postgresql.Driver")
                .load()
            )
            return df
        except Exception as e:
            logger.warning("Não foi possível carregar ref_payment_type via JDBC: %s. Utilizando fallback em memória.", e)
            return None

    def load_month(self, df: DataFrame, year_month: str) -> None:
        """
        Espera um DataFrame de uma única competência yyyyMM.
        A staging é carregada via JDBC; depois, uma transação no PostgreSQL
        substitui atomicamente apenas a competência processada.
        """
        staging_table = "silver.yellow_taxi_trips_stage"
        target_table = "silver.yellow_taxi_trips"

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
                if self._table_exists(cur, staging_table):
                    cur.execute(f"TRUNCATE TABLE {staging_table}")
            conn.commit()

        # Adiciona o timestamp de processamento exigido pelo schema PostgreSQL
        df = df.withColumn("processed_at", F.current_timestamp())

        # Grava os dados tratados no staging do PostgreSQL
        (
            df.write
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", staging_table)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

        # Transação atômica de troca de partição (DELETE + INSERT)
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target_table}
                    (LIKE {staging_table} INCLUDING ALL)
                """)
                cur.execute(
                    f"DELETE FROM {target_table} WHERE pickup_year_month = %s",
                    (year_month,),
                )
                cur.execute(f"""
                    INSERT INTO {target_table}
                    SELECT * FROM {staging_table}
                    WHERE pickup_year_month = %s
                """, (year_month,))
                cur.execute(f"TRUNCATE TABLE {staging_table}")
            conn.commit()
        logger.info("Competência %s carregada com sucesso em %s", year_month, target_table)

    def refresh_materialized_view(self) -> None:
        """Atualiza a visão materializada da camada Gold."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW gold.mv_monthly_taxi_metrics;")
            conn.commit()
        logger.info("Materialized View gold.mv_monthly_taxi_metrics atualizada.")

    @staticmethod
    def _table_exists(cursor, qualified_table: str) -> bool:
        schema, table = qualified_table.split(".", 1)
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table))
        return cursor.fetchone()[0]
