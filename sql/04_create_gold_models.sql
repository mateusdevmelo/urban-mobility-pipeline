-- 04_create_gold_models.sql
-- Modelagem analítica da Camada Gold (Star Schema e Materialized View)

-- 1. Dimensão de Fornecedores / Frotistas (Vendor)
CREATE TABLE IF NOT EXISTS gold.dim_vendor (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL,
    vendor_code VARCHAR(20) NOT NULL
);

INSERT INTO gold.dim_vendor (vendor_id, vendor_name, vendor_code)
VALUES
    (1, 'Creative Mobile Technologies, LLC', 'CMT'),
    (2, 'VeriFone Inc.', 'VTS'),
    (0, 'Unknown Vendor', 'UNKNOWN')
ON CONFLICT (vendor_id) DO UPDATE
SET vendor_name = EXCLUDED.vendor_name,
    vendor_code = EXCLUDED.vendor_code;

INSERT INTO gold.dim_vendor (vendor_id, vendor_name, vendor_code)
SELECT DISTINCT
    COALESCE(s."VendorID", 0) AS vendor_id,
    'Other Vendor (' || COALESCE(s."VendorID", 0)::text || ')' AS vendor_name,
    'V' || COALESCE(s."VendorID", 0)::text AS vendor_code
FROM silver.yellow_taxi_trips s
WHERE COALESCE(s."VendorID", 0) NOT IN (1, 2, 0)
ON CONFLICT (vendor_id) DO NOTHING;

-- 2. Dimensão de Formas de Pagamento
CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
    payment_type INTEGER PRIMARY KEY,
    payment_description VARCHAR(50) NOT NULL,
    is_valid_payment BOOLEAN NOT NULL
);

INSERT INTO gold.dim_payment_type (payment_type, payment_description, is_valid_payment)
SELECT payment_type, payment_description, is_valid_payment
FROM silver.ref_payment_type
ON CONFLICT (payment_type) DO UPDATE
SET payment_description = EXCLUDED.payment_description,
    is_valid_payment = EXCLUDED.is_valid_payment;

-- Garante que o código 5 (Unknown) e código 0 existam
INSERT INTO gold.dim_payment_type (payment_type, payment_description, is_valid_payment)
VALUES
    (5, 'Unknown', FALSE),
    (0, 'Unknown', FALSE)
ON CONFLICT (payment_type) DO NOTHING;

-- 3. Dimensão de Data / Calendário
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year_month VARCHAR(6) NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Popula dim_date dinamicamente a partir dos embarques da silver
INSERT INTO gold.dim_date (
    date_key, year, month, day, quarter, year_month, day_of_week, day_name, is_weekend
)
SELECT DISTINCT
    pickup_date AS date_key,
    EXTRACT(YEAR FROM pickup_date)::INTEGER AS year,
    EXTRACT(MONTH FROM pickup_date)::INTEGER AS month,
    EXTRACT(DAY FROM pickup_date)::INTEGER AS day,
    EXTRACT(QUARTER FROM pickup_date)::INTEGER AS quarter,
    TO_CHAR(pickup_date, 'YYYYMM') AS year_month,
    EXTRACT(ISODOW FROM pickup_date)::INTEGER AS day_of_week,
    TO_CHAR(pickup_date, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM pickup_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM silver.yellow_taxi_trips
WHERE pickup_date IS NOT NULL
ON CONFLICT (date_key) DO NOTHING;

-- 4. Tabela Fato de Corridas (Fato)
CREATE TABLE IF NOT EXISTS gold.fct_taxi_trips (
    trip_id VARCHAR(64) PRIMARY KEY,
    vendor_id INTEGER REFERENCES gold.dim_vendor(vendor_id),
    payment_type INTEGER REFERENCES gold.dim_payment_type(payment_type),
    pickup_date DATE REFERENCES gold.dim_date(date_key),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_year_month VARCHAR(6) NOT NULL,
    passenger_count DOUBLE PRECISION,
    trip_distance NUMERIC(10, 2),
    trip_duration_minutes NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    valid_revenue_amount NUMERIC(10, 2),
    is_valid_trip BOOLEAN NOT NULL,
    is_valid_payment BOOLEAN NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    invalid_reason TEXT,
    anomaly_reason TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_fct_ym ON gold.fct_taxi_trips (pickup_year_month);
CREATE INDEX IF NOT EXISTS idx_gold_fct_vendor ON gold.fct_taxi_trips (vendor_id);
CREATE INDEX IF NOT EXISTS idx_gold_fct_pickup_date ON gold.fct_taxi_trips (pickup_date);

-- Carga / Atualização da Fato a partir da Silver com garantia de integridade referencial e unicidade
INSERT INTO gold.fct_taxi_trips (
    trip_id,
    vendor_id,
    payment_type,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_year_month,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    total_amount,
    valid_revenue_amount,
    is_valid_trip,
    is_valid_payment,
    is_anomaly,
    invalid_reason,
    anomaly_reason
)
SELECT DISTINCT ON (s.trip_id)
    s.trip_id,
    COALESCE(s."VendorID", 0) AS vendor_id,
    CASE WHEN s.payment_type IN (1, 2, 3, 4, 5, 6, 0) THEN s.payment_type ELSE 5 END AS payment_type,
    s.pickup_date,
    s.pickup_datetime,
    s.dropoff_datetime,
    s.pickup_year_month,
    s.passenger_count,
    ROUND(s.trip_distance::numeric, 2),
    ROUND(s.trip_duration_minutes::numeric, 2),
    ROUND(s.fare_amount::numeric, 2),
    ROUND(s.total_amount::numeric, 2),
    ROUND(s.valid_revenue_amount::numeric, 2),
    s.is_valid_trip,
    s.is_valid_payment,
    s.is_anomaly,
    s.invalid_reason,
    s.anomaly_reason
FROM silver.yellow_taxi_trips s
ORDER BY s.trip_id, s.pickup_datetime
ON CONFLICT (trip_id) DO UPDATE
SET
    vendor_id = EXCLUDED.vendor_id,
    payment_type = EXCLUDED.payment_type,
    pickup_date = EXCLUDED.pickup_date,
    trip_distance = EXCLUDED.trip_distance,
    trip_duration_minutes = EXCLUDED.trip_duration_minutes,
    fare_amount = EXCLUDED.fare_amount,
    total_amount = EXCLUDED.total_amount,
    valid_revenue_amount = EXCLUDED.valid_revenue_amount,
    is_valid_trip = EXCLUDED.is_valid_trip,
    is_valid_payment = EXCLUDED.is_valid_payment,
    is_anomaly = EXCLUDED.is_anomaly,
    invalid_reason = EXCLUDED.invalid_reason,
    anomaly_reason = EXCLUDED.anomaly_reason;

-- 5. Materialized View de Indicadores Mensais por Vendor (Requisito 7)
DROP MATERIALIZED VIEW IF EXISTS gold.mv_monthly_taxi_metrics;

CREATE MATERIALIZED VIEW gold.mv_monthly_taxi_metrics AS
SELECT
    f.pickup_year_month,
    f.vendor_id,
    v.vendor_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(CASE WHEN f.is_valid_trip AND f.is_valid_payment THEN f.total_amount ELSE 0 END)::numeric, 2) AS total_valid_revenue,
    ROUND(AVG(CASE WHEN f.is_valid_trip AND f.is_valid_payment THEN f.total_amount ELSE NULL END)::numeric, 2) AS avg_ticket_per_trip,
    ROUND(AVG(CASE WHEN f.is_valid_trip THEN f.trip_distance ELSE NULL END)::numeric, 2) AS avg_trip_distance,
    CURRENT_TIMESTAMP AS last_refreshed_at
FROM gold.fct_taxi_trips f
LEFT JOIN gold.dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY f.pickup_year_month, f.vendor_id, v.vendor_name
ORDER BY f.pickup_year_month, f.vendor_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_mv_monthly_taxi_metrics
ON gold.mv_monthly_taxi_metrics (pickup_year_month, vendor_id);
