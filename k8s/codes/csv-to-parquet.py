import logging
import sys
import ast

import pyspark.sql.functions as f

from pyspark.sql import SparkSession
from variables import PATH_SOURCE, PATH_TARGET

class CSVtoPARQUET:
    def __init__(self, spark, path_source:str, format_source: str, path_target:str, format_target: str) -> None:
        self.spark = spark

        if format_source != 'csv':
            raise Exception(f"The format_source {format_source} is not supported. Use CSV.")
        elif format_target != 'parquet':
            raise Exception(f"The format_target {format_target} is not supported. Use PARQUET.")
        else:
            self.format_source = format_source
            self.format_target = format_target
        
        self.path_source = path_source
        self.path_target = path_target
    
    def run(self) -> str:
        self.create_logger()
        self.csv_to_parquet()

        return "Application completed. Going out..."

    def create_logger(self):
        logging.basicConfig(format='%(name)s - %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p', stream=sys.stdout)
        logger = logging.getLogger('ETL_AWS_VINICIUS_CAMPOS')
        logger.setLevel(logging.DEBUG)

    def csv_to_parquet(self):
        df = (
            self.spark.read.format(self.format_source)
            .option("sep", ",")
            .option("header", True)
            .option("encoding", "utf-8")
            .load(self.path_source)
        )

        renamed_df = df.select([f.col(col).alias(col.replace(' ', '_')) for col in df.columns])

        return renamed_df.coalesce(1).write.mode("overwrite").format(self.format_target).save(self.path_target)

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName('ETL_AWS_VINICIUS_CAMPOS')
        .enableHiveSupport()
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
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
        .config('spark.sql.join.preferSortMergeJoin', 'true')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    script_input = ast.literal_eval(sys.argv[1])
    
    file = script_input['file']
    format_source = script_input['format_source']
    format_target = script_input['format_target']

    m = CSVtoPARQUET(
        spark, 
        PATH_SOURCE.format(file=file), 
        format_source,
        PATH_TARGET.format(file=file),
        format_target
    )

    m.run()

    spark.stop()