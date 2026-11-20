from __future__ import annotations

import json
from pathlib import Path
import pytest

from src.ingestion.downloader import TLCDownloader, TLC_BASE_URL


def test_downloader_init(tmp_path: Path):
    bronze_dir = tmp_path / "bronze"
    manifest_file = tmp_path / "manifest.json"

    downloader = TLCDownloader(bronze_dir, manifest_file)
    assert bronze_dir.exists()
    assert manifest_file.parent.exists()


def test_downloader_invalid_month(tmp_path: Path):
    downloader = TLCDownloader(tmp_path / "bronze", tmp_path / "manifest.json")
    with pytest.raises(ValueError, match="month deve estar entre 1 e 12"):
        downloader.download_month(2025, 0)

    with pytest.raises(ValueError, match="month deve estar entre 1 e 12"):
        downloader.download_month(2025, 13)


def test_downloader_invalid_period(tmp_path: Path):
    downloader = TLCDownloader(tmp_path / "bronze", tmp_path / "manifest.json")
    with pytest.raises(ValueError, match="start_month não pode ser maior que end_month"):
        downloader.download_period(2025, 6, 2)


def test_downloader_sha256(tmp_path: Path):
    test_file = tmp_path / "sample.txt"
    test_file.write_text("Urban Mobility Analytics TLC Test Data", encoding="utf-8")

    digest = TLCDownloader._sha256(test_file)
    assert isinstance(digest, str)
    assert len(digest) == 64


def test_downloader_manifest_operations(tmp_path: Path):
    manifest_file = tmp_path / "metadata" / "manifest.json"
    downloader = TLCDownloader(tmp_path / "bronze", manifest_file)

    manifest_data = {
        "2025-01": {
            "file": "path/to/file",
            "url": "http://example.com",
            "size_bytes": 1024,
            "sha256": "fakehash",
            "status": "downloaded",
        }
    }
    downloader._save_manifest(manifest_data)
    loaded = downloader._load_manifest()
    assert loaded == manifest_data


def test_downloader_skips_when_in_manifest(tmp_path: Path):
    bronze_dir = tmp_path / "bronze"
    manifest_file = tmp_path / "manifest.json"
    downloader = TLCDownloader(bronze_dir, manifest_file)

    dest = bronze_dir / "year=2025" / "month=01" / "yellow_tripdata_2025-01.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("mock content", encoding="utf-8")

    downloader._save_manifest({
        "2025-01": {
            "file": str(dest),
            "size_bytes": dest.stat().st_size,
            "status": "downloaded",
        }
    })

    # Chamada sem force deve retornar o arquivo existente sem tentar baixar via HTTP
    result = downloader.download_month(2025, 1, force=False)
    assert result == dest
