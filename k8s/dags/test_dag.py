import boto3
import json

from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3_bucket import (
    S3CreateBucketOperator, 
    S3DeleteBucketOperator
)
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

################################### VARIABLES ##########################################################

AWS_PROJECT = getenv("AWS_PROJECT", "vini-etl-aws")
REGION = getenv("REGION", "us-east-1")

CODE_PATH = 's3://emr-code-zone-vini-etl-aws'

################################### SPARK_CLUSTER_CONFIG ################################################

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
                "InstanceCount": 1,
            },
            {
                "Name": "TASK_NODES",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "AutoScalingPolicy":
                    {
                        "Constraints":
                    {
                        "MinCapacity": 1,
                        "MaxCapacity": 2
                    },
                    "Rules":
                        [
                    {
                    "Name": "Scale Up",
                    "Action":{
                        "SimpleScalingPolicyConfiguration":{
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": 1,
                        "CoolDown": 120
                        }
                    },
                    "Trigger":{
                        "CloudWatchAlarmDefinition":{
                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                        "EvaluationPeriods": 1,
                        "MetricName": "Scale Up",
                        "Period": 60,
                        "Threshold": 15,
                        "Statistic": "AVERAGE",
                        "Threshold": 75
                        }
                    }
                    }
                    ]
                }
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
    'StepConcurrencyLevel': 1
}

################################### SPARK_ARGUMENTS #####################################################

SPARK_ARGUMENTS = [
    'spark-submit',
    '--deploy-mode', 'cluster',
    '--conf', 'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2',
    '--conf', 'spark.sql.join.preferSortMergeJoin=true',
    '--conf', 'spark.speculation=false',
    '--conf', 'spark.sql.adaptive.enabled=true',
    '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
    '--conf', 'spark.sql.adaptive.coalescePartitions.minPartitionNum=1',
    '--conf', 'spark.sql.adaptive.coalescePartitions.initialPartitionNum=10',
    '--conf', 'spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728',
    '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
    '--conf', 'spark.dynamicAllocation.minExecutors=5',
    '--conf', 'spark.dynamicAllocation.maxExecutors=30',
    '--conf', 'spark.dynamicAllocation.initialExecutors=10'
]

CSV_TO_PARQUET_ARGS = [
    '--py-files', f'{CODE_PATH}/variables.py',
    f'{CODE_PATH}/csv-to-parquet.py'
]

################################### FUNCTIONS #####################################################

def trigger_lambda():

    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION
    )

    response = lambda_client.invoke(
        FunctionName='myfunction',
        InvocationType='Event',
        LogType='None',
        Qualifier='$LATEST'
    )

    response_json = json.dumps(response, default=str)

    return response_json

def add_spark_step(dag, aux_args, job_id, params=None):

    args = SPARK_ARGUMENTS.copy()
    args.extend(aux_args)

    if params:
        args.append(json.dumps(params))

    steps = [{
        "Name": f"Converting CSV to Parquet - Job {job_id}",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
             "Args": args
        }
    }]

    task = EmrAddStepsOperator(
        task_id=f'csv_to_parquet_{job_id}',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=steps,
        aws_conn_id='aws',
        dag=dag
    )

    return task

def get_all_csvs_in_bucket():

    bucket = 'landing-zone-vini-etl-aws'
    prefix = 'data/'

    s3 = boto3.client(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='us-east-1'
    )

    csv_files = []

    for files in s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']:
        key = files['Key']
        if key.endswith(".csv"):
            csv_files.append(key.split("/")[-1].split(".")[0])
    
    return csv_files

################################### LISTS #####################################################

buckets = [
    'landing-zone',
    'processing-zone',
    'curated-zone'
]

################################### TASKS #####################################################

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
    concurrency=1,
    max_active_runs=1,
    catchup=False
) as dag:

    task_lambda = PythonOperator(
        task_id='trigger_lambda',
        python_callable=trigger_lambda
    )

    for bucket in buckets:
        create_buckets = S3CreateBucketOperator(
            task_id=f'create_bucket_{bucket}'+f'-{AWS_PROJECT}',
            bucket_name=bucket+f'-{AWS_PROJECT}',
            region_name=REGION,
            aws_conn_id='aws'
        )
        create_buckets >> task_lambda

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws",
        emr_conn_id="emr",
        region_name=REGION,
        dag=dag,
    )

    emr_create_sensor = EmrJobFlowSensor(
        task_id='monitoring_emr_cluster_creation',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
        aws_conn_id="aws"
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule="all_done",
        aws_conn_id="aws"
    )

    for file in get_all_csvs_in_bucket():
        task_csv_to_parquet = add_spark_step(
            dag,
            CSV_TO_PARQUET_ARGS,
            f'{file}',
            params={
                'file': file, 
                'format_source': 'csv', 
                'format_target': 'parquet'
            }
        )

        step_checker = EmrStepSensor(
            task_id=f'watch_step_{file}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='csv_to_parquet_{file}', key='return_value')[0] }}".format(file=file),
            target_states=['COMPLETED'],
            failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
            aws_conn_id="aws",
            dag=dag
        )

        (
            task_lambda >> create_emr_cluster >> 
            
            emr_create_sensor >> task_csv_to_parquet >> step_checker >> terminate_emr_cluster
        )