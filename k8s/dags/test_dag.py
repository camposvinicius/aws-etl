import pandas as pd
import boto3
import json
import io

from os import getenv
from datetime import timedelta
from sqlalchemy import create_engine

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago

################################### OPERATORS ###########################################################

from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.s3_bucket import (
    S3CreateBucketOperator, 
    S3DeleteBucketOperator
)
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

################################### VARIABLES ###########################################################

AWS_PROJECT = getenv("AWS_PROJECT", "vini-etl-aws")

REGION = getenv("REGION", "us-east-1")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

LANDING_ZONE = getenv('LANDING_ZONE', f'landing-zone-{AWS_PROJECT}')
CURATED_ZONE = getenv('CURATED_ZONE', f'curated-zone-{AWS_PROJECT}')
CURATED_KEY = getenv('CURATED_KEY', 'curated/')

REDSHIFT_USER = getenv("REDSHIFT_USER", "vini")
REDSHIFT_SCHEMA = getenv("REDSHIFT_SCHEMA", "vini_etl_aws_redshift_schema")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "vini_etl_aws_redshift_table")

ATHENA_TABLE = getenv("ATHENA_TABLE", "curated")
ATHENA_DATABASE = getenv("ATHENA_DATABASE", "vini-database-etl-aws")
ATHENA_OUTPUT = getenv("ATHENA_OUTPUT", "s3://athena-results-vini-etl-aws/")

POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD")
POSTGRES_USERNAME = 'vinietlaws'
POSTGRES_PORT = '5432'
POSTGRES_DATABASE = 'vinipostgresql'
POSTGRESQL_TABLE = 'vini_etl_aws_postgresql_table'
POSTGRES_ENDPOINT = f'{POSTGRES_DATABASE}-instance.cngltutuixt3.us-east-1.rds.amazonaws.com'

POSTGRESQL_CONNECTION = f'postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ENDPOINT}:{POSTGRES_PORT}/{POSTGRES_DATABASE}'

EMR_CODE_PATH = 's3://emr-code-zone-vini-etl-aws'

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
                "Market": "SPOT",
                "BidPrice": "0.078",
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
        'Ec2KeyName': 'my-key',
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
    '--py-files', f'{EMR_CODE_PATH}/variables.py',
    f'{EMR_CODE_PATH}/csv-to-parquet.py'
]

SEND_TO_CURATED = [
    '--py-files', f'{EMR_CODE_PATH}/variables.py',
    f'{EMR_CODE_PATH}/transformation.py'
]

################################### LISTS #####################################################

csv_files = [
  'Customers',
  'Product_Categories',
  'Product_Subcategories',
  'Products',
  'Returns',
  'Sales_2015',
  'Sales_2016',
  'Sales_2017'
]

buckets = [
    'landing-zone',
    'processing-zone',
    'curated-zone'
]

################################### FUNCTIONS ###########################################################

def trigger_lambda():

    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
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

def add_spark_step(dag, aux_args, job_id, task_id, params=None):

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
        task_id=task_id,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=steps,
        aws_conn_id='aws',
        dag=dag
    )

    return task

def write_on_postgres():
   
    s3_client = boto3.client('s3', 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION
    )

    s3 = boto3.resource('s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION
    )

    parquet_list = []

    objects = s3_client.list_objects_v2(Bucket=CURATED_ZONE)

    for obj in objects['Contents']:
        parquet_list.append(obj['Key'])

    key = parquet_list[-1]
    buffer = io.BytesIO()
    object = s3.Object(CURATED_ZONE, key)
    object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)

    engine = create_engine(POSTGRESQL_CONNECTION)
    df.to_sql(f'{POSTGRESQL_TABLE}', engine, schema='public', if_exists='replace', index=False)

def on_failure_callback(context):
    task_sns = SnsPublishOperator(
        task_id='on_failure_callback',
        target_arn='target_arn',
        message="Dag Failed",
        subject="Dag Failed",
        aws_conn_id='aws'
    )

    task_sns.execute()
################################### TASKS #####################################################

default_args = {
    'owner': 'Vini Campos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': on_failure_callback,
    'retries': 1
}

with DAG(
    dag_id="vini-campos-etl-aws",
    tags=['etl', 'aws', 'dataengineer'],
    default_args=default_args,
    start_date=days_ago(1),
    on_failure_callback=on_failure_callback,
    schedule_interval='@daily',
    concurrency=10,
    max_active_runs=1,
    catchup=False
) as dag:

    task_lambda = PythonOperator(
        task_id='trigger_lambda',
        python_callable=trigger_lambda,
        execution_timeout=timedelta(seconds=120)
    )

    verify_csv_files_on_s3 = S3KeySensor(
        task_id='verify_csv_files_on_s3',
        bucket_key='data/AdventureWorks/*.csv',
        wildcard_match=True,
        bucket_name=LANDING_ZONE,
        aws_conn_id='aws',
        soft_fail=False,
        poke_interval=15,
        timeout=60,
        dag=dag
    )

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

    task_send_to_curated = add_spark_step(
        dag,
        SEND_TO_CURATED,
        'task_send_to_curated',
        'task_send_to_curated',
    )

    step_checker_curated = EmrStepSensor(
        task_id=f'watch_task_send_to_curated',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='task_send_to_curated', key='return_value')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
        aws_conn_id="aws",
        dag=dag
    )

    create_schema_redshift = RedshiftSQLOperator(
        task_id='create_schema_redshift',
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA} AUTHORIZATION {REDSHIFT_USER} QUOTA 2048 MB;
        """,
        redshift_conn_id='redshift'
    )

    create_table_redshift = RedshiftSQLOperator(
        task_id='create_table_redshift',
        sql=f""" 
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (
                OrderDate date,
                StockDate date,
                CustomerKey int,
                TerritoryKey int,
                OrderLineItem int,
                OrderQuantity int,
                Prefix varchar,
                FirstName varchar,
                LastName varchar,
                BirthDate date,
                MaritalStatus varchar,
                Gender varchar,
                EmailAddress varchar,
                AnnualIncome decimal(10,2),
                TotalChildren int,
                EducationLevel varchar,
                Occupation varchar,
                HomeOwner varchar,
                ProductKey int,
                ProductSubcategoryKey int,
                SubcategoryName varchar,
                ProductCategoryKey int,
                CategoryName varchar,
                ProductSKU varchar,
                ProductName varchar,
                ModelName varchar,
                ProductDescription varchar,
                ProductColor varchar,
                ProductSize int,
                ProductStyle varchar,
                ProductCost decimal(10,2),
                ProductPrice decimal(10,2),
                ReturnDate date,
                ReturnQuantity int
            );
        """,
        redshift_conn_id='redshift'
    )

    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        s3_bucket=CURATED_ZONE,
        s3_key=CURATED_KEY,
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_TABLE,
        aws_conn_id='aws',
        redshift_conn_id='redshift',
        copy_options=['parquet']
    )

    create_schema_redshift >> create_table_redshift >> s3_to_redshift

    write_data_on_postgres = PythonOperator(
        task_id='write_data_on_postgres',
        python_callable=write_on_postgres
    )

    verify_table_count = PostgresOperator(
        task_id=f'verify_{POSTGRESQL_TABLE}_count',
        sql=f""" 
            SELECT 
                count(*)
            FROM
                {POSTGRESQL_TABLE}
            );
        """,
        postgres_conn_id='postgres',
        database=POSTGRES_DATABASE
    )

    write_data_on_postgres >> verify_table_count

    glue_crawler = AwsGlueCrawlerOperator(
        task_id='glue_crawler_curated',
        config={"Name": "CrawlerETLAWSVini"},
        aws_conn_id='aws',
        poll_interval=10
    )

    athena_verify_table_count = AWSAthenaOperator(
        task_id='athena_verify_table_count',
        query=f""" 
            SELECT 
                count(*)
            FROM
                "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
        """,
        database=f'{ATHENA_DATABASE}',
        output_location=f'{ATHENA_OUTPUT}',
        do_xcom_push=True,
        aws_conn_id='aws'
    )

    athena_query_sensor = AthenaSensor(
        task_id='athena_query_sensor',
        query_execution_id="{{ task_instance.xcom_pull(task_ids='athena_verify_table_count', key='return_value') }}",
        aws_conn_id='aws'
    )

    glue_crawler >> athena_verify_table_count >> athena_query_sensor

    for bucket in buckets:
        create_buckets = S3CreateBucketOperator(
            task_id=f'create_bucket_{bucket}'+f'_{AWS_PROJECT}',
            bucket_name=bucket+f'-{AWS_PROJECT}',
            region_name=REGION,
            aws_conn_id='aws'
        )
        create_buckets >> task_lambda >> verify_csv_files_on_s3

        #delete_buckets = S3DeleteBucketOperator(
        #    task_id=f'delete_bucket_{bucket}',
        #    bucket_name=bucket,
        #    force_delete=True,
        #    aws_conn_id='aws'
        #)

        s3_to_redshift #>> delete_buckets

    for file in csv_files:
        task_csv_to_parquet = add_spark_step(
            dag,
            CSV_TO_PARQUET_ARGS,
            f'{file}',
            f'csv_to_parquet_{file}',
            params={
                'file': f'AdventureWorks_{file}', 
                'format_source': 'csv', 
                'format_target': 'parquet'
            }
        )

        step_checker = EmrStepSensor(
            task_id=f'watch_step_{file}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='csv_to_parquet_{file}', key='return_value')[0] }}}}",
            target_states=['COMPLETED'],
            failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
            aws_conn_id="aws",
            dag=dag
        )


        (
            verify_csv_files_on_s3 >> create_emr_cluster >> 
            
            emr_create_sensor >> task_csv_to_parquet >> step_checker >> 
            
            task_send_to_curated >> step_checker_curated >>
            
            [terminate_emr_cluster, glue_crawler, create_schema_redshift, write_data_on_postgres]
        )
