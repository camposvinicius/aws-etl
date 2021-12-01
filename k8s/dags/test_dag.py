from os import getenv

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3_bucket import (
    S3CreateBucketOperator, 
    S3DeleteBucketOperator
)
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

AWS_PROJECT = getenv("AWS_PROJECT", "vini-etl-aws")
REGION = getenv("REGION", "us-east-1")

default_args = {
    'owner': 'Vini Campos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    dag_id="vini-campos-etl-aws",
    tags=['etl', 'aws', 'dataengineer'],
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    JOB_FLOW_OVERRIDES = {
        'Name': 'ETL-VINI-AWS',
        "ReleaseLabel": "emr-6.5.0",
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], 
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'MASTER_NODES',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    "Name": "CORE_NODES",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
                {
                    "Name": "TASK_NODES",
                    "BidPrice": "0.313",
                    "Market": "SPOT",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                }            
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
    }

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws",
        emr_conn_id="emr",
        dag=dag,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws"
    )

    buckets = [
        'landing-zone',
        'processing-zone',
        'curated-zone'
    ]

    for bucket in buckets:
        create_buckets = S3CreateBucketOperator(
            task_id=f'create_bucket_{bucket}'+f'_{AWS_PROJECT}',
            bucket_name=bucket+f'_{AWS_PROJECT}',
            aws_conn_id='aws',
            region_name=REGION
        )

        create_buckets >> create_emr_cluster >> terminate_emr_cluster