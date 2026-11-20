-- 03_create_silver_tables.sql
-- Definição das tabelas da camada Silver (staging e definitiva)

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_trips (
    trip_id VARCHAR(64) PRIMARY KEY,
    "VendorID" INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    "RatecodeID" DOUBLE PRECISION,
    store_and_fwd_flag VARCHAR(10),
    "PULocationID" INTEGER,
    "DOLocationID" INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    "Airport_fee" DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_duration_minutes DOUBLE PRECISION,
    pickup_date DATE,
    pickup_year_month VARCHAR(6),
    payment_description VARCHAR(50),
    is_valid_payment BOOLEAN,
    invalid_reason TEXT,
    is_valid_trip BOOLEAN,
    is_anomaly BOOLEAN,
    anomaly_reason TEXT,
    valid_revenue_amount DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_trips_ym ON silver.yellow_taxi_trips (pickup_year_month);
CREATE INDEX IF NOT EXISTS idx_silver_trips_date ON silver.yellow_taxi_trips (pickup_date);
CREATE INDEX IF NOT EXISTS idx_silver_trips_vendor ON silver.yellow_taxi_trips ("VendorID");
CREATE INDEX IF NOT EXISTS idx_silver_trips_payment ON silver.yellow_taxi_trips (payment_type);

-- Tabela de Staging para carga atômica de lotes mensais
CREATE TABLE IF NOT EXISTS silver.yellow_taxi_trips_stage (
    LIKE silver.yellow_taxi_trips INCLUDING ALL
);
