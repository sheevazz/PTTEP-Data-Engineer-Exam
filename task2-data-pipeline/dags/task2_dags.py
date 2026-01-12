from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    "task2_gcs_to_bq",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    run_etl = CloudRunExecuteJobOperator(
        task_id="run_task2_etl",
        project_id="gcp-test-data-engineer",
        region="asia-southeast1",
        job_name="task2-ingest"
    )
