-- Migration: add normalized value and quality flags to stg.openaq_measurements_by_location
-- Run with psql (or in a Flyway/DBT/etc. change) against the warehouse DB.
-- Example: psql "$WAREHOUSE_DB_DSN" -f scripts/migrate_openaq_measurements_add_quality.sql

BEGIN;

-- Add columns if they do not already exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'stg' AND table_name = 'openaq_measurements_by_location' AND column_name = 'value_normalized'
    ) THEN
        ALTER TABLE stg.openaq_measurements_by_location
            ADD COLUMN value_normalized NUMERIC NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'stg' AND table_name = 'openaq_measurements_by_location' AND column_name = 'is_valid'
    ) THEN
        ALTER TABLE stg.openaq_measurements_by_location
            ADD COLUMN is_valid BOOLEAN NOT NULL DEFAULT TRUE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'stg' AND table_name = 'openaq_measurements_by_location' AND column_name = 'quality_status'
    ) THEN
        ALTER TABLE stg.openaq_measurements_by_location
            ADD COLUMN quality_status TEXT NULL;
    END IF;
END $$;

-- Backfill normalized values and quality flags from existing raw value
UPDATE stg.openaq_measurements_by_location
SET
    value_normalized = CASE
        WHEN value IS NULL THEN NULL
        WHEN value < 0 THEN NULL
        ELSE value
    END,
    is_valid = CASE
        WHEN value IS NULL THEN FALSE
        WHEN value < 0 THEN FALSE
        ELSE TRUE
    END,
    quality_status = CASE
        WHEN value IS NULL THEN 'missing_value'
        WHEN value < 0 THEN 'negative_value'
        ELSE NULL
    END
WHERE
    value_normalized IS NULL
    OR is_valid IS NULL
    OR quality_status IS NULL;

COMMIT;
