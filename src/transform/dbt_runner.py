
from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)


class DbtRunner:
    """
    Executa a transformação da camada Silver para Gold via dbt.

    A camada Gold é construída e validada exclusivamente pelo dbt.
    Não existe fallback automático para SQL nativo.

    O executável dbt pode ser configurado pela variável:
        DBT_EXECUTABLE

    Caso não esteja configurada, procura o dbt no PATH.
    """

    def __init__(self, project_root: Path | None = None):
        self.project_root = (
            Path(project_root).resolve()
            if project_root
            else Path(__file__).resolve().parents[2]
        )

        self.dbt_dir = self.project_root / "dbt"
        self.sql_dir = self.project_root / "sql"

        self.pg_host = os.environ.get("POSTGRES_HOST", "localhost")
        self.pg_port = os.environ.get("POSTGRES_PORT", "5433")
        self.pg_db = os.environ.get(
            "POSTGRES_DB",
            "urban_mobility_db",
        )
        self.pg_user = os.environ.get("POSTGRES_USER", "postgres")
        self.pg_password = os.environ.get("POSTGRES_PASSWORD")

    # ---------------------------------------------------------
    # PostgreSQL
    # ---------------------------------------------------------

    def _get_pg_connection(self):
        """Abre uma conexão com o PostgreSQL."""

        if not self.pg_password:
            raise RuntimeError(
                "POSTGRES_PASSWORD não configurada nas variáveis "
                "de ambiente."
            )

        return psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            dbname=self.pg_db,
            user=self.pg_user,
            password=self.pg_password,
            connect_timeout=10,
        )

    # ---------------------------------------------------------
    # DBT EXECUTABLE
    # ---------------------------------------------------------

    def _get_dbt_executable(self) -> str | None:
        """
        Localiza o executável do dbt.

        Prioridade:
        1. Caminho definido em DBT_EXECUTABLE.
        2. Executável dbt disponível no PATH.
        """

        configured_path = os.environ.get("DBT_EXECUTABLE")

        if configured_path:
            dbt_path = Path(configured_path).expanduser()

            if dbt_path.is_file():
                return str(dbt_path.resolve())

            logger.warning(
                "DBT_EXECUTABLE configurado, mas o arquivo "
                "não foi encontrado: %s",
                dbt_path,
            )

        return shutil.which("dbt")

    def is_dbt_available(self) -> bool:
        """Verifica se o executável do dbt está disponível."""

        executable = self._get_dbt_executable()

        if executable:
            logger.debug("Executável dbt encontrado: %s", executable)
            return True

        logger.warning("Executável do dbt não encontrado.")

        return False

    # ---------------------------------------------------------
    # DBT COMMAND EXECUTION
    # ---------------------------------------------------------

    def _execute_dbt_command(
        self,
        command: str,
        select: str | None = None,
    ) -> bool:
        """
        Executa um comando dbt.

        Exemplos:
            dbt build
            dbt test
        """

        executable = self._get_dbt_executable()

        if not executable:
            logger.error(
                "Não foi possível executar dbt %s: "
                "executável não encontrado.",
                command,
            )
            return False

        if not self.dbt_dir.is_dir():
            logger.error(
                "Diretório do projeto dbt não encontrado: %s",
                self.dbt_dir,
            )
            return False

        project_file = self.dbt_dir / "dbt_project.yml"

        if not project_file.is_file():
            logger.error(
                "Arquivo dbt_project.yml não encontrado: %s",
                project_file,
            )
            return False

        cmd = [
            executable,
            command,
            "--project-dir",
            str(self.dbt_dir),
            "--profiles-dir",
            str(self.dbt_dir),
        ]

        if select:
            cmd.extend(["--select", select])

        logger.info(
            "Executando comando: %s",
            " ".join(cmd),
        )

        env = os.environ.copy()

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_root),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        except FileNotFoundError:
            logger.exception(
                "Executável dbt não encontrado durante a execução."
            )
            return False

        except subprocess.TimeoutExpired:
            logger.exception(
                "Tempo limite excedido na execução do dbt."
            )
            return False

        except OSError:
            logger.exception(
                "Erro do sistema operacional ao executar o dbt."
            )
            return False

        if result.stdout:
            logger.info(
                "Saída do dbt:\n%s",
                result.stdout,
            )

        if result.stderr:
            if result.returncode == 0:
                logger.warning(
                    "Avisos do dbt:\n%s",
                    result.stderr,
                )
            else:
                logger.error(
                    "Erros do dbt:\n%s",
                    result.stderr,
                )

        if result.returncode != 0:
            logger.error(
                "Comando dbt %s falhou. Código de saída: %s",
                command,
                result.returncode,
            )
            return False

        logger.info(
            "Comando dbt %s executado com sucesso.",
            command,
        )

        return True

    def run_dbt(self, select: str | None = None) -> bool:
        """Executa dbt build."""

        return self._execute_dbt_command(
            command="build",
            select=select,
        )

    def test_dbt(self) -> bool:
        """Executa dbt test."""

        return self._execute_dbt_command(
            command="test",
        )

    # ---------------------------------------------------------
    # LEGACY SQL METHOD
    # ---------------------------------------------------------

    def run_sql_transformations(self) -> None:
        """
        Método legado para execução de SQL nativo.

        Não é utilizado automaticamente pelo build().
        """

        gold_sql_file = (
            self.sql_dir / "04_create_gold_models.sql"
        )

        if not gold_sql_file.is_file():
            raise FileNotFoundError(
                f"Arquivo SQL não encontrado: {gold_sql_file}"
            )

        logger.info(
            "Executando modelagem Gold via SQL nativo: %s",
            gold_sql_file,
        )

        sql_content = gold_sql_file.read_text(
            encoding="utf-8"
        )

        with self._get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_content)

            conn.commit()

        logger.info(
            "Camada Gold gerada com sucesso via SQL nativo."
        )

    # ---------------------------------------------------------
    # MATERIALIZED VIEW
    # ---------------------------------------------------------

    def refresh_materialized_view(self) -> None:
        """Atualiza gold.mv_monthly_taxi_metrics."""

        logger.info(
            "Atualizando Materialized View "
            "gold.mv_monthly_taxi_metrics..."
        )

        with self._get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    REFRESH MATERIALIZED VIEW
                    gold.mv_monthly_taxi_metrics;
                    """
                )

            conn.commit()

        logger.info(
            "Materialized View atualizada com sucesso."
        )

    # ---------------------------------------------------------
    # GOLD BUILD
    # ---------------------------------------------------------

    def build(self) -> None:
        """
        Constrói e valida a camada Gold exclusivamente pelo dbt.

        Não permite fallback automático para SQL nativo.
        """

        if not self.is_dbt_available():
            raise RuntimeError(
                "dbt não encontrado no ambiente. "
                "Configure DBT_EXECUTABLE ou o PATH."
            )

        logger.info(
            "Iniciando dbt build para construção "
            "e validação da camada Gold..."
        )

        success = self.run_dbt()

        if not success:
            raise RuntimeError(
                "Falha na execução do dbt build. "
                "Verifique os logs antes de continuar."
            )

        logger.info(
            "Camada Gold construída e validada "
            "com sucesso pelo dbt."
        )

    # ---------------------------------------------------------
    # GOLD METRICS
    # ---------------------------------------------------------

    def fetch_gold_metrics(self) -> list[dict[str, Any]]:
        """Consulta os indicadores da Materialized View Gold."""

        query = """
            SELECT
                pickup_year_month,
                vendor_id,
                vendor_name,
                total_trips,
                total_valid_revenue,
                avg_ticket_per_trip,
                avg_trip_distance
            FROM gold.mv_monthly_taxi_metrics
            ORDER BY pickup_year_month, vendor_id;
        """

        with self._get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)

                rows = cur.fetchall()

                cols = [
                    description[0]
                    for description in cur.description
                ]

                return [
                    dict(zip(cols, row))
                    for row in rows
                ]
