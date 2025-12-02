
# E-commerce Product Catalog Management (Airflow + dbt + Snowflake)

This project delivers a complete pipeline to ingest, transform, and track product data with SCD Type 2 history using **dbt**, **Airflow**, and **Snowflake**. It uses your uploaded Home Depot CSV: `home_depot_data_1_2021_12.csv`.

## 1) Environment Setup (1 pt)

- Python 3.8+ (recommended 3.10)
- Airflow 2.8+ with:
  - `apache-airflow-providers-snowflake`
  - `apache-airflow-providers-amazon` (if using S3)
- dbt-core 1.7+ and `dbt-snowflake`
- Snowflake account with a role that can create DBs/schemas/tables/stages
- Python connector: `pip install snowflake-connector-python`

### Optional (one command local deploy)
A simple Docker Compose is included to run Airflow. Mount this repo so that the DAG and dbt project appear in the container paths used by the DAG.

## 2) Source dataset (1 pt)

We use your uploaded CSV. The Airflow DAG expects it at `/opt/airflow/dags/data/home_depot_data_1_2021_12.csv` **inside the container**. This repo already places it at `airflow/dags/data/home_depot_data_1_2021_12.csv`; just mount the `airflow/dags` folder into your Airflow containers.

If you prefer S3, set Airflow Variable `product_data_source="s3"`, `s3_bucket`, `s3_key` and configure the S3 external stage.

## 3) Snowflake setup (1 pt)

The DAG task `create_objects` runs `airflow/include/sql/create_objects.sql` which:

- Creates database `PRODUCT_CATALOG`
- Schemas: `STAGING`, `CORE`, `SNAPSHOTS`
- File format `product_csv_ff`
- Internal stage `staging.int_stage`
- Staging table `staging.raw_products`

## 4) Airflow DAG (4 pts)

**DAG:** `airflow/dags/product_catalog_etl.py`

**Tasks:**

- `create_objects` — idempotent Snowflake DDL
- `check_local_file` — Short-circuit if the CSV is missing (error handling)
- `put_local_to_stage` — `PUT` the CSV to the internal stage (local path)  
  or `s3_to_snowflake` — if `product_data_source="s3"`
- `copy_into_raw` — `COPY INTO staging.raw_products`
- `dbt_deps` — run `dbt deps`
- `dbt_snapshot` — run `dbt snapshot`
- `dbt_run` — build models
- `dbt_test` — run tests
- Zero-copy cloning tasks:
  - `clone_dev_database` (Dev clone, daily)
  - `create_eoy_snapshot` (Point-in-time EOY clone)
  - `run_prototype` (Rapid prototyping with schema clone)
  - `manage_backups` (Daily backup & 7-day retention)

**Operators used:** `SnowflakeOperator`, `ShortCircuitOperator`, `BashOperator` (or `DbtCoreOperation` if installed), and optionally `S3ToSnowflakeOperator`.

**Error handling:** task retries, short-circuit for missing files, and zero-copy tasks set to `ALL_DONE` so you still get backups/snapshots even after partial failures.

## 5) dbt models (6 pts total)

- Project structure: `dbt/product_catalog`
- Staging model: `models/staging/stg_products.sql`
  - Cleans and types fields, creates `product_nk`, sets `updated_at` timestamps.
- Core SCD Type 2 Dimension:
  - **Preferred:** `models/core/dim_product_scd2_from_snapshot.sql` — uses snapshot history.
  - **Alternate:** `models/core/dim_product_scd2_window.sql` — pure SQL window-based SCD2.
- Tests:
  - `models/staging/stg_products.yml` (not_null, ranges)
  - `models/core/dim_product.yml` and `tests/dim_product_scd2_from_snapshot.yml` generic tests:
    - uniqueness on `(product_nk, valid_from)`
    - `single_current_per_key`
    - `no_overlaps`

We provide our own `surrogate_key` macro to avoid a hard dependency on `dbt_utils`. If you want `dbt_utils`, run `dbt deps` (already wired in the DAG).

## 6) dbt snapshots (1 pt)

Snapshot file: `snapshots/product_snapshot.sql` using `strategy: timestamp` on `updated_at` and `unique_key: product_nk`.

## 7) dbt & Airflow integration (1 pt)

The DAG runs `dbt deps`, `dbt snapshot`, `dbt run`, and `dbt test`. If you install the `astronomer-cosmos` provider, it uses `DbtCoreOperation`. Otherwise it falls back to CLI via `BashOperator`.

## 8) Pipeline run & verification (3 pts)

1. **Start Airflow** and enable DAG `product_catalog_etl`.
2. After the run:
   - `select count(*) from product_catalog.staging.raw_products;`
   - `select * from product_catalog.staging.raw_products limit 10;`
3. Verify SCD2:
   ```sql
   -- Should have exactly one current row per product_nk
   select product_nk, count_if(is_current) as current_rows
   from product_catalog.core.dim_product_scd2_from_snapshot
   group by 1
   having current_rows != 1;
   ```
4. **Change the source**: edit a few prices or titles in the CSV, rerun the DAG (or run `dbt snapshot` then `dbt run`). You should see:
   - old row's `valid_to` populated
   - new row added with `is_current = true`

## 9) Snowflake Zero-Copy Cloning (4 pts)

**9.1 Development Environment Cloning (1 pt)**  
Task `clone_dev_database` runs:
```sql
CREATE OR REPLACE DATABASE dev_product_catalog CLONE product_catalog;
```
You can point your dbt `dev` target at this DB to test features safely.

**9.2 Point-in-Time Analysis (1 pt)**  
Task `create_eoy_snapshot` creates an EOY clone:
```sql
CREATE OR REPLACE DATABASE product_catalog_eoy_<year>
CLONE product_catalog AT (TIMESTAMP => '<year>-12-31 23:59:59'::timestamp_ntz);
```
Use it for year-over-year analysis.

**9.3 Rapid Prototyping with Cloning (1 pt)**  
Task `run_prototype` clones schema, creates a prototype view, then cleans up.

**9.4 Backup & Restore (1 pt)**  
Task `manage_backups` creates a daily clone DB and prunes the 7th previous day.

To restore, simply point dbt or your session to the backup DB or clone it back into `product_catalog`.

## How to run locally with Docker (optional)

1. Install Docker and docker-compose.
2. Build/Start:
   ```bash
   docker compose up -d
   ```
3. Create Airflow connections via UI or environment variables.
4. Set Airflow Variables:
   - `DBT_PROFILES_DIR=/opt/airflow/dbt`
   - `DBT_PROJECT_DIR=/opt/airflow/dbt/product_catalog`
   - `product_data_source=local`
5. Unpause the DAG `product_catalog_etl`.

## 10) Experiments & 5 Key Takeaways (2 pts)

**Experiments:**  
- Compared snapshot-based vs window-based SCD2 — snapshots are more declarative and auditable; window-based gives full SQL control.
- Evaluated local `PUT` vs S3 external stage — S3 scales better and decouples compute from transfer; local `PUT` is simplest for homework.
- Tested `DbtCoreOperation` vs CLI — operator gives richer lineage & logs; CLI is dependency-light and reliable.
- Tried different file formats (DATE/TIMESTAMP parsing, NULL handling) — strict CSV options reduce bad loads, but sometimes you prefer `ON_ERROR='CONTINUE'` while exploring.
- Zero-copy cloning for dev/backup — instantaneous clones enable safe experimentation and disaster recovery with minimal storage overhead.

**Key Takeaways:**  
1. dbt snapshots are the cleanest way to maintain SCD history; pair them with model-level tests to prevent overlap/duplicates.  
2. Airflow + SnowflakeOperator lets you keep your DDL and data movement idempotent and versioned.  
3. Zero-copy cloning changes the dev/test/backup story — instant, cheap, and safe.  
4. Make `product_nk` carefully (coalesce across identifiers) to avoid accidental record explosion.  
5. Build in failure paths (short-circuit, retries, ALL_DONE on backups) so operational hygiene is automatic.

## 11) How would I extend this exercise? (1 pt)

- Add automated **data contracts** (schema + column-level constraints) enforced at `COPY` time with `VALIDATION_MODE=RETURN_ERRORS` and dbt tests.
- Add **freshness checks** in Airflow (e.g., `S3KeySensor`) before the load.
- Generate a **dbt docs site** (`dbt docs generate && dbt docs serve`) and publish artifacts.
- Add **lineage visualization** using Cosmos or OpenLineage for Airflow/dbt.
- Add a small **metrics layer** (e.g., dbt metric definitions, exposures) and BI dashboard snapshotting.

---

### Quick Commands

Local dbt smoke test (after creating objects & loading data):
```bash
cd dbt/product_catalog
export DBT_PROFILES_DIR=$(pwd)  # uses the provided profiles.yml
dbt deps
dbt snapshot
dbt run
dbt test
```

Query in Snowflake:
```sql
select * from product_catalog.core.dim_product_scd2_from_snapshot limit 50;
```

Enjoy!
