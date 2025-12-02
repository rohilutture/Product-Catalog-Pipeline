# airflow/dags/dbt_build.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os, requests

# ---------- Error handling (see section 2 below) ----------
from airflow.utils.email import send_email
def alert(context):
    send_email(
        to="you@example.com",
        subject=f"Airflow failure: {context['task_instance_key_str']}",
        html_content=str(context),
    )

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert,
}

# ---------- Extract: download file to the shared /usr/app volume ----------
def download_to_local(**ctx):
    url = ctx["params"]["file_url"]  # presigned S3 or any reachable URL
    out = "/usr/app/data/home_depot_data_1_2021_12.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with open(out, "wb") as f:
        f.write(r.content)
    return out

with DAG(
    dag_id="dbt_build",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "snowflake"],
    default_args=default_args,   # <— add default_args here
) as dag:

    # 1) Extract
    extract = PythonOperator(
        task_id="extract_dataset",
        python_callable=download_to_local,
        params={
            # TODO: replace with your real file URL (or keep if you already have a file somewhere)
            "file_url": "https://<your-presigned-or-hosted-file>.csv"
        },
    )

    # 2) Load (COPY INTO from your internal stage)
    # If you upload to the internal stage (STG_UPLOAD) outside of Airflow, you can still run this COPY step here.
    copy_into_raw = SnowflakeOperator(
        task_id="copy_into_raw",
        snowflake_conn_id="snowflake_conn",
        sql="""
        COPY INTO PRODUCT_CATALOG.STAGING.RAW_PRODUCTS
        FROM @PRODUCT_CATALOG.STAGING.STG_UPLOAD
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            SKIP_HEADER=1
            NULL_IF=('','NULL')
            EMPTY_FIELD_AS_NULL=TRUE
        )
        ON_ERROR = 'ABORT_STATEMENT';
        """,
    )

    # NOTE: If you want to also automate *upload to the stage*, that requires SnowSQL or using an *external*
    # S3 stage + S3 upload from Airflow. Since your compose already includes the AWS provider,
    # you can switch to an external stage later. For now this keeps the assignment’s "Load" step in Airflow.

    # 3) dbt deps
    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.7.2",
        command="bash -c 'cd /usr/app/dbt/product_catalog && dbt deps'",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(source="product_catalog_pipeline_app_data", target="/usr/app", type="volume")],
        environment={"DBT_PROFILES_DIR": "/usr/app/dbt"},
    )

    # 4) dbt build
    dbt_build_task = DockerOperator(
        task_id="dbt_build_task",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.7.2",
        command="bash -c 'cd /usr/app/dbt/product_catalog && dbt debug && dbt build'",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(source="product_catalog_pipeline_app_data", target="/usr/app", type="volume")],
        environment={
            "DBT_PROFILES_DIR": "/usr/app/dbt",
            "SF_ACCOUNT": "{{ var.value.SF_ACCOUNT }}",
            "SF_USER": "{{ var.value.SF_USER }}",
            "SF_PASSWORD": "{{ var.value.SF_PASSWORD }}",
            "SF_ROLE": "{{ var.value.SF_ROLE }}",
            "SF_WAREHOUSE": "{{ var.value.SF_WAREHOUSE }}",
        },
    )

    # Orchestration order
    extract >> copy_into_raw >> dbt_deps >> dbt_build_task
