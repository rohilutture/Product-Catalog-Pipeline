
"""
Airflow DAG: product_catalog_etl

End-to-end pipeline:
- Create Snowflake database / schemas / objects (idempotent)
- Stage the CSV (local PUT or S3)
- COPY INTO staging.raw_products
- Run dbt (deps -> snapshot -> run -> test)
- Zero-copy cloning tasks (optional, can be scheduled separately)

Prereqs:
  Airflow connections:
    - snowflake_conn (Conn Type: Snowflake) with account, user, role, warehouse, database
    - aws_default (if using S3ToSnowflakeOperator path)

Airflow Variables (optional, with defaults in code):
  - product_data_source: "local" | "s3"
  - s3_bucket: "your-bucket"
  - s3_key: "path/to/home_depot_data_1_2021_12.csv"
  - DBT_PROFILES_DIR: e.g. "/opt/airflow/dbt"
  - DBT_PROJECT_DIR: e.g. "/opt/airflow/dbt/product_catalog"
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Optional: use S3ToSnowflakeOperator if you place the file in S3 and have a stage configured
try:
    from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
    HAS_S3_TO_SNOWFLAKE = True
except Exception:
    HAS_S3_TO_SNOWFLAKE = False

# Optional: Use dbt-core operator (cosmos). If missing, we fallback to BashOperator.
try:
    from cosmos.providers.dbt.task_group import DbtTaskGroup  # not used here
    from cosmos.providers.dbt.core.operators import DbtCoreOperation
    HAS_DBT_CORE_OP = True
except Exception:
    HAS_DBT_CORE_OP = False

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def file_exists(**context):
    path = "/opt/airflow/dags/data/home_depot_data_1_2021_12.csv"
    exists = os.path.exists(path)
    if not exists:
        print(f"Expected CSV at {path} was not found. Mount the project's 'airflow/dags/data' folder into the scheduler/webserver/worker containers.")
    return exists

with DAG(
    dag_id="product_catalog_etl",
    default_args=default_args,
    description="ETL for Product Catalog with SCD Type 2 (dbt + Snowflake)",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["product-catalog", "dbt", "snowflake", "scd2"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Create DB/Schema/Tables/File formats/Stages (idempotent)
    create_objects = SnowflakeOperator(
        task_id="create_objects",
        snowflake_conn_id="snowflake_conn",
        sql="include/sql/create_objects.sql",
    )

    # Determine source (local vs S3) and load to stage
    data_source = Variable.get("product_data_source", default_var="local").lower()

    check_local_file = ShortCircuitOperator(
        task_id="check_local_file",
        python_callable=file_exists,
        ignore_downstream_trigger_rules=False,
    )

    # For local: use internal stage + PUT
    put_local_to_stage = SnowflakeOperator(
        task_id="put_local_to_stage",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            -- Use internal stage created in create_objects.sql
            REMOVE @product_catalog.staging.int_stage PATTERN='.*home_depot_data_1_2021_12.*';
            PUT file:///opt/airflow/dags/data/home_depot_data_1_2021_12.csv @product_catalog.staging.int_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
        """,
    )

    # For S3: direct transfer to table (if present)
    if HAS_S3_TO_SNOWFLAKE:
        s3_to_snowflake = S3ToSnowflakeOperator(
            task_id="s3_to_snowflake",
            snowflake_conn_id="snowflake_conn",
            s3_keys=[Variable.get("s3_key", default_var="path/to/home_depot_data_1_2021_12.csv")],
            stage="product_catalog.staging.s3_stage",
            table="raw_products",
            schema="staging",
            file_format="product_csv_ff",
        )
    else:
        s3_to_snowflake = EmptyOperator(task_id="s3_to_snowflake_skipped")

    # COPY FROM stage to table
    copy_into_raw = SnowflakeOperator(
        task_id="copy_into_raw",
        snowflake_conn_id="snowflake_conn",
        sql="""
            TRUNCATE TABLE product_catalog.staging.raw_products;
            COPY INTO product_catalog.staging.raw_products
            FROM @product_catalog.staging.int_stage
            FILE_FORMAT = (FORMAT_NAME='product_catalog.staging.product_csv_ff')
            PATTERN='.*home_depot_data_1_2021_12.*'
            ON_ERROR='CONTINUE';
        """,
    )

    # --- dbt steps ---
    # Resolve dirs
    DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/opt/airflow/dbt")
    DBT_PROJECT_DIR  = Variable.get("DBT_PROJECT_DIR", default_var="/opt/airflow/dbt/product_catalog")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_snapshot_cmd = f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"
    dbt_run_cmd      = f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}"
    dbt_test_cmd     = f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}"

    if HAS_DBT_CORE_OP:
        dbt_snapshot = DbtCoreOperation(
            task_id="dbt_snapshot",
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
            select=["snapshot:*"],
            command="snapshot",
        )
        dbt_run = DbtCoreOperation(
            task_id="dbt_run",
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
            select=["tag:core"],
            command="run",
        )
        dbt_test = DbtCoreOperation(
            task_id="dbt_test",
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
            command="test",
        )
    else:
        dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=dbt_snapshot_cmd)
        dbt_run = BashOperator(task_id="dbt_run", bash_command=dbt_run_cmd)
        dbt_test = BashOperator(task_id="dbt_test", bash_command=dbt_test_cmd)

    # --- Zero-copy cloning tasks (Daily dev clone; Year-end snapshot; Prototype; Backup management) ---
    clone_dev_database = SnowflakeOperator(
        task_id="clone_dev_database",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE DATABASE dev_product_catalog CLONE product_catalog;
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_eoy_snapshot = SnowflakeOperator(
        task_id="create_eoy_snapshot",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE DATABASE product_catalog_eoy_{{ ds[:4] }}
            CLONE product_catalog AT(TIMESTAMP => '{{ ds[:4] }}-12-31 23:59:59'::timestamp_ntz);
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_prototype = SnowflakeOperator(
        task_id="run_prototype",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE SCHEMA product_catalog.prototype_schema CLONE product_catalog.core;
            USE SCHEMA product_catalog.prototype_schema;
            -- Example: create a view with a hypothetical new column (rating)
            CREATE OR REPLACE VIEW dim_product_prototype AS
            SELECT *, NULL::NUMBER(3,2) AS rating
            FROM product_catalog.core.dim_product_scd2;
            -- Clean up prototype schema
            DROP SCHEMA product_catalog.prototype_schema;
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    manage_backups = SnowflakeOperator(
        task_id="manage_backups",
        snowflake_conn_id="snowflake_conn",
        sql="""
            -- Create today's backup
            CREATE OR REPLACE DATABASE product_catalog_backup_{{ ds_nodash }} CLONE product_catalog;
            -- Drop backup older than 7 days
            DROP DATABASE IF EXISTS product_catalog_backup_{{ macros.ds_add(ds, -7) | replace('-', '') }};
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end")

    # Orchestration logic based on source
    if data_source == "s3" and HAS_S3_TO_SNOWFLAKE:
        start >> create_objects >> s3_to_snowflake >> dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test >> [clone_dev_database, create_eoy_snapshot, run_prototype, manage_backups] >> end
    else:
        # local path
        start >> create_objects >> check_local_file >> put_local_to_stage >> copy_into_raw >> dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test >> [clone_dev_database, create_eoy_snapshot, run_prototype, manage_backups] >> end
