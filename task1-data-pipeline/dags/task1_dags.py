from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.run import CloudRunJobOperator

with DAG(
    "task1_gcs_to_bq",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_task1 = CloudRunJobOperator(
        task_id="run_task1_ingest",
        project_id="gcp-test-data-engineer",
        region="asia-southeast1",
        job_name="task1-ingest"
)
