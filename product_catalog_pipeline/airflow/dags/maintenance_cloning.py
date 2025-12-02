# airflow/dags/maintenance_cloning.py
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    dag_id="maintenance_cloning",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",         # adjust per task needs; you can split into separate DAGs if preferred
    catchup=False,
    tags=["snowflake", "cloning"],
) as dag:

    # 9.1 Dev clone (daily)
    clone_dev_db = SnowflakeOperator(
        task_id='clone_dev_database',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE DATABASE dev_product_catalog CLONE product_catalog;
        ALTER DATABASE dev_product_catalog SET DATA_RETENTION_TIME_IN_DAYS = 30;
        """,
    )

    # 9.2 End-of-year snapshot (30-day retention)
    create_eoy_snapshot = SnowflakeOperator(
        task_id="create_eoy_snapshot",
        snowflake_conn_id="snowflake_conn",
        sql="""
        CREATE OR REPLACE DATABASE product_catalog_eoy_{{ logical_date.year }}
        CLONE product_catalog;
        ALTER DATABASE product_catalog_eoy_{{ logical_date.year }} 
        SET DATA_RETENTION_TIME_IN_DAYS = 30;
        """,
    )

    # 9.3 Prototype schema clone (on-demand testing)
    prototype_test = SnowflakeOperator(
        task_id='run_prototype',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE SCHEMA prototype_schema CLONE product_catalog.public;
        ALTER SCHEMA prototype_schema SET DATA_RETENTION_TIME_IN_DAYS = 30;

        -- Run any test SQL or model verification here

        DROP SCHEMA prototype_schema;
        """,
    )

    # 9.4 Backup & prune (keep 7 days)
    manage_backups = SnowflakeOperator(
        task_id='manage_backups',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE DATABASE product_catalog_backup_{{ ds_nodash }} CLONE product_catalog;
        ALTER DATABASE product_catalog_backup_{{ ds_nodash }} SET DATA_RETENTION_TIME_IN_DAYS = 30;

        -- Drop backups older than 7 days
        DROP DATABASE IF EXISTS product_catalog_backup_{{ macros.ds_add(ds, -7) | replace('-', '') }};
        """,
    )

    # Optional dependency chain (linear order)
    clone_dev_db >> create_eoy_snapshot >> prototype_test >> manage_backups
    verify_retention = SnowflakeOperator(
    task_id='verify_retention',
    snowflake_conn_id='snowflake_conn',
    sql="""
    SHOW PARAMETERS IN DATABASE PRODUCT_CATALOG_EOY_{{ logical_date.year }};
    """,
)
manage_backups >> verify_retention

