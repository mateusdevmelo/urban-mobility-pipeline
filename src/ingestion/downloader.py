from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


class TLCDownloader:
    """Baixa arquivos mensais Yellow Taxi da NYC TLC e mantém um manifesto local e no PostgreSQL."""

    def __init__(self, bronze_dir: str | Path, manifest_path: str | Path):
        self.bronze_dir = Path(bronze_dir)
        self.manifest_path = Path(manifest_path)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def _load_manifest(self) -> dict:
        if not self.manifest_path.exists():
            return {}
        try:
            return json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Falha ao ler manifesto local: %s. Iniciando novo.", e)
            return {}

    def _save_manifest(self, manifest: dict) -> None:
        self.manifest_path.write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _sync_to_db(self, file_name: str, year_month: str, url: str, path: Path, size: int, sha256: str) -> None:
        """Registra metadados do arquivo na tabela bronze.ingestion_manifest se o banco estiver disponível."""
        pg_host = os.getenv("POSTGRES_HOST")
        pg_db = os.getenv("POSTGRES_DB")
        if not (pg_host and pg_db):
            return

        try:
            import psycopg2
            with psycopg2.connect(
                host=pg_host,
                port=os.getenv("POSTGRES_PORT", "5433"),
                dbname=pg_db,
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD"),
                connect_timeout=3,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS bronze.ingestion_manifest (
                            file_name VARCHAR(255) PRIMARY KEY,
                            year_month VARCHAR(6) NOT NULL,
                            url TEXT NOT NULL,
                            file_path TEXT NOT NULL,
                            size_bytes BIGINT NOT NULL,
                            sha256 VARCHAR(64) NOT NULL,
                            status VARCHAR(50) DEFAULT 'downloaded',
                            ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cur.execute("""
                        INSERT INTO bronze.ingestion_manifest
                            (file_name, year_month, url, file_path, size_bytes, sha256, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_name) DO UPDATE
                        SET size_bytes = EXCLUDED.size_bytes,
                            sha256 = EXCLUDED.sha256,
                            status = EXCLUDED.status;
                    """, (file_name, year_month, url, str(path), size, sha256, "downloaded"))
                conn.commit()
            logger.info("Metadados registrados em bronze.ingestion_manifest no PostgreSQL.")
        except Exception as err:
            logger.debug("Banco de dados não conectado para auditoria bronze (%s). Prosseguindo com manifesto local.", err)

    def download_month(self, year: int, month: int, force: bool = False) -> Path:
        if not (1 <= month <= 12):
            raise ValueError("month deve estar entre 1 e 12")
        if year < 2000 or year > 2100:
            raise ValueError(f"Ano inválido: {year}")

        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        destination = self.bronze_dir / f"year={year}" / f"month={month:02d}" / filename
        destination.parent.mkdir(parents=True, exist_ok=True)

        manifest = self._load_manifest()
        key = f"{year}-{month:02d}"
        ym = f"{year}{month:02d}"

        if destination.exists() and key in manifest and not force:
            logger.info("Arquivo já existe no manifesto local; ignorando download: %s", destination)
            return destination

        url = f"{TLC_BASE_URL}/{filename}"
        temporary = destination.with_suffix(".parquet.part")
        logger.info("Baixando %s para %s", url, destination)

        request = Request(url, headers={"User-Agent": "UrbanMobilityAnalytics/1.0"})
        try:
            with urlopen(request, timeout=120) as response, temporary.open("wb") as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
            if temporary.stat().st_size == 0:
                raise RuntimeError(f"Download retornou arquivo vazio: {url}")
            temporary.replace(destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

        checksum = self._sha256(destination)
        size_bytes = destination.stat().st_size
        manifest[key] = {
            "file": str(destination),
            "url": url,
            "size_bytes": size_bytes,
            "sha256": checksum,
            "status": "downloaded",
        }
        self._save_manifest(manifest)
        self._sync_to_db(filename, ym, url, destination, size_bytes, checksum)
        logger.info("Download concluído com sucesso: %s (%s bytes)", destination, size_bytes)
        return destination

    def download_period(self, year: int, start_month: int, end_month: int, force: bool = False) -> list[Path]:
        if start_month > end_month:
            raise ValueError("start_month não pode ser maior que end_month")
        return [
            self.download_month(year, month, force=force)
            for month in range(start_month, end_month + 1)
        ]
