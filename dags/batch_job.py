import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="aws_batch_job",
    description="Trigger AWS Batch Job",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    start_date=dt.datetime.today(),
)


def trigger_batch_job_aws():
    """Trigger AWS Batch job in AWS."""
    import boto3

    client = boto3.client("batch")
    response = client.submit_job(
        jobName="Ana_capstone_job",
        jobQueue="arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2023-job-queue",
        jobDefinition="arn:aws:batch:eu-west-1:338791806049:job-definition/Ana-capstone:2",
    )
    print(response)


with dag:
    t1 = PythonOperator(
        task_id="aws_batch_job_capstone_ana",
        python_callable=trigger_batch_job_aws,
        dag=dag,
    )
