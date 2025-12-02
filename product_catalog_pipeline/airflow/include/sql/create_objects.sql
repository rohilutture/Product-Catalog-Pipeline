
-- Idempotent creation of DB/SCHEMAS/TABLES/STAGES/FILE FORMATS for the project
CREATE DATABASE IF NOT EXISTS product_catalog;

-- Schemas
CREATE SCHEMA IF NOT EXISTS product_catalog.staging;
CREATE SCHEMA IF NOT EXISTS product_catalog.core;
CREATE SCHEMA IF NOT EXISTS product_catalog.snapshots;

-- File format for CSV
CREATE FILE FORMAT IF NOT EXISTS product_catalog.staging.product_csv_ff
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  DATE_FORMAT = 'AUTO'
  TIME_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';

-- Internal stage (for local PUT)
CREATE STAGE IF NOT EXISTS product_catalog.staging.int_stage
  FILE_FORMAT = product_catalog.staging.product_csv_ff;

-- Optional: S3 external stage (configure STORAGE INTEGRATION separately if used)
-- CREATE STAGE IF NOT EXISTS product_catalog.staging.s3_stage
--   URL='s3://your-bucket/path/'
--   STORAGE_INTEGRATION=your_storage_integration
--   FILE_FORMAT = product_catalog.staging.product_csv_ff;

-- RAW staging table matching the CSV schema (Home Depot dataset)
CREATE TABLE IF NOT EXISTS product_catalog.staging.raw_products (
  index           NUMBER,
  url             STRING,
  title           STRING,
  images          STRING,
  description     STRING,
  product_id      STRING,
  sku             STRING,
  gtin13          STRING,
  brand           STRING,
  price           STRING,
  currency        STRING,
  availability    STRING,
  uniq_id         STRING,
  scraped_at      STRING
);

-- Cleaned staging view (dbt will create stg_products view, but this is handy for ad hoc)
-- CREATE OR REPLACE VIEW product_catalog.staging.stg_products AS
-- SELECT ... (dbt model will build this)
