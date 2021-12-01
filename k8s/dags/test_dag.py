from os import getenv

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_PROJECT_ID = getenv("GCP_PROJECT_ID", "teste-zero-vini")
REGION = getenv("REGION", "us-east1")
REGION_CLUSTER = getenv("REGION_CLUSTER", "us-east4")
LOCATION = getenv("LOCATION", "us-east1")
CURATED_BUCKET_ZONE = getenv("CURATED_BUCKET_ZONE", f"curated-{GCP_PROJECT_ID}")
PYSPARK_URI = getenv("PYSPARK_URI", f"gs://codes-{GCP_PROJECT_ID}/codes-zone/challenge.py")
PYFILES_ZIP_URI =  getenv("PYFILES_ZIP_URI", f"gs://codes-{GCP_PROJECT_ID}/codes-zone/pyfiles.zip")
AVRO_JAR_URI =  getenv("AVRO_JAR_URI", f"gs://codes-{GCP_PROJECT_ID}/codes-zone/spark-avro_2.12-3.1.2.jar")
DATAPROC_CLUSTER_NAME = getenv("DATAPROC_CLUSTER_NAME", f"dataproc-{GCP_PROJECT_ID}-challenge")
BQ_DATASET_NAME = getenv("BQ_DATASET_NAME", "TestZeroVini")
BQ_TABLE_NAME = getenv("BQ_TABLE_NAME", "TestZeroViniTable")

default_args = {
    'owner': 'Vinicius Campos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    dag_id="challenge-marktplaats",
    tags=['challenge', 'marktplaats'],
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    dp_cluster = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100}, },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100}, },
    }

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER_NAME,
        cluster_config=dp_cluster,
        region=REGION_CLUSTER,
        use_if_exists=True,
        gcp_conn_id="gcp"
    )

    py_spark_job_submit = DataprocSubmitPySparkJobOperator(
        task_id="py_spark_job_submit",
        main=PYSPARK_URI,
        arguments=['avro'],
        pyfiles=[PYFILES_ZIP_URI],
        dataproc_jars=[AVRO_JAR_URI],
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=REGION_CLUSTER,
        asynchronous=True,
        gcp_conn_id="gcp"
    )

    dataproc_job_sensor = DataprocJobSensor(
        task_id="dataproc_job_sensor",
        project_id=GCP_PROJECT_ID,
        region=REGION_CLUSTER,
        dataproc_job_id="{{ task_instance.xcom_pull(key='job_conf', task_ids='py_spark_job_submit')['job_id'] }}",
        poke_interval=5,
        gcp_conn_id="gcp"
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        region=REGION_CLUSTER,
        cluster_name=DATAPROC_CLUSTER_NAME,
        gcp_conn_id="gcp"
    )
    dataproc_job_sensor >> delete_dataproc_cluster

    bq_create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="bq_create_dataset",
        dataset_id=BQ_DATASET_NAME,
        gcp_conn_id="gcp"
    )

    ingest_df_into_bq_table = GCSToBigQueryOperator(
        task_id="ingest_df_into_bq_table",
        bucket=CURATED_BUCKET_ZONE,
        source_objects=['consume-zone/*.avro'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        source_format='avro',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
        bigquery_conn_id="gcp"
    )

    check_bq_tb_count = BigQueryCheckOperator(
        task_id="check_bq_tb_count",
        sql=f""" 
                SELECT 
                    count(*) 
                FROM 
                    {BQ_DATASET_NAME}.{BQ_TABLE_NAME} 
            """,
        use_legacy_sql=False,
        location="us",
        gcp_conn_id="gcp"
    )
    dataproc_job_sensor >> bq_create_dataset >> ingest_df_into_bq_table >> check_bq_tb_count

    
    create_dataproc_cluster >> py_spark_job_submit >> dataproc_job_sensor