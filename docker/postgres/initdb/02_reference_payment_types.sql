CREATE TABLE IF NOT EXISTS silver.ref_payment_type (
    payment_type INTEGER PRIMARY KEY,
    payment_description TEXT NOT NULL,
    is_valid_payment BOOLEAN NOT NULL
);

INSERT INTO silver.ref_payment_type
    (payment_type, payment_description, is_valid_payment)
VALUES
    (1, 'Credit card', TRUE),
    (2, 'Cash', TRUE),
    (3, 'No charge', FALSE),
    (4, 'Dispute', FALSE),
    (5, 'Unknown', FALSE),
    (6, 'Voided trip', FALSE),
    (0, 'Unknown', FALSE)
ON CONFLICT (payment_type) DO UPDATE
SET payment_description = EXCLUDED.payment_description,
    is_valid_payment = EXCLUDED.is_valid_payment;
