import logging
import sys
import re

from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import HiveContext

AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID", "AKIAS263V23PQSCLEIO7")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY", "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")

class SparktoDynamoDB:
    def __init__(self, spark) -> None:
        self.spark = spark

    def run(self) -> str:
        self.create_logger()
        self.df_spark_to_dynamodb()

        return "Application completed. Going out..."

    def create_logger(self):
        logging.basicConfig(format='%(name)s - %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p', stream=sys.stdout)
        logger = logging.getLogger('ETL_AWS_VINICIUS_CAMPOS')
        logger.setLevel(logging.DEBUG)

    def df_spark_to_dynamodb(self):

        sc = spark.SparkContext()

        sqlContext = HiveContext(sc)

        df = (
            self.spark.read
            .option("sep", ",")
            .option("header", True)
            .format("csv")
            .load("/home/vinicius_campos/airflow-docker/countries.csv")
        )

        df = df.select([expr(f"`{col_name}` as {col_name.replace(' ', '_')}") for col_name in df.columns])
        df.createOrReplaceTempView("df")

        columns = (','.join([field.simpleString() for field in df.schema])).replace(':', ' ')

        spark.sql(f""" 
            CREATE TABLE IF NOT EXISTS TEMP (
                {columns}
            )
            STORED AS ORC;
        """)

        spark.sql(""" INSERT OVERWRITE TABLE TEMP SELECT * FROM df """)

        cols_aux_list = []

        cols_aux = re.split('[: ,]', columns)

        for col in cols_aux:
            if col != 'string':
                cols_aux_list.append(col)
        
        cols_dynamo = []

        for col in cols_aux_list:
            cols_dynamo.append(col + ":" + col)

        cols_dynamo = ', '.join(cols_dynamo)

        spark.sql(f""" 
        
            CREATE TABLE IF NOT EXISTS TEMPTODYNAMO (
                {columns}
            )
            STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
            TBLPROPERTIES (
                "dynamodb.table.name" = "vini-etl-aws-dynamodb-table",
                "dynamodb.region" = "us-east-1",
                "dynamodb.column.mapping" = "{cols_dynamo}");
        """)

        spark.sql(""" INSERT OVERWRITE TABLE TEMPTODYNAMO SELECT * FROM TEMP """)

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName('ETL_AWS_VINICIUS_CAMPOS')
        .enableHiveSupport()
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.fast.upload", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config('spark.speculation', 'false')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.shuffle.service.enabled', 'true')
        .config('spark.dynamicAllocation.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.minPartitionNum', '1')
        .config('spark.sql.adaptive.coalescePartitions.initialPartitionNum', '10')
        .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '134217728')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .config('spark.dynamicAllocation.minExecutors', "5")
        .config('spark.dynamicAllocation.maxExecutors', "30")
        .config('spark.dynamicAllocation.initialExecutors', "10")
        .config('spark.sql.debug.maxToStringFields', '300')
        .config('spark.jars', 'emr-ddb-hive.jar,hive-serde-2.3.6-amzn-1.jar')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    m = SparktoDynamoDB(spark)

    m.run()

    spark.stop()