-- 01_create_schemas.sql
-- Criação dos schemas para a arquitetura medalhão e catálogo de ingestão bronze

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Tabela de auditoria / catálogo da camada Bronze no PostgreSQL
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
