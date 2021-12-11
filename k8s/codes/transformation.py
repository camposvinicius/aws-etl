import logging
import sys

from pyspark.sql import SparkSession
from variables import PATH_TARGET, PATH_CURATED, QUERY, VIEWS

class ServeData:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.path_target = PATH_TARGET
        self.path_curated = PATH_CURATED
        self.query = QUERY
    
    def run(self) -> str:
        self.create_logger()
        self.to_curated()

        return "Application completed. Going out..."

    def create_logger(self):
        logging.basicConfig(format='%(name)s - %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p', stream=sys.stdout)
        logger = logging.getLogger('ETL_AWS_VINICIUS_CAMPOS')
        logger.setLevel(logging.DEBUG)

    def to_curated(self):

        views_to_drop = []

        for view in VIEWS:
            print(view)
            (
                spark.read.format("parquet")
                .load(f'{self.path_target}'.format(file=view))
                .createOrReplaceTempView(f'{view}')
            )
            views_to_drop.append(view)

        print(views_to_drop)
        
        df = spark.sql(self.query['QUERY'])

        for view in views_to_drop:
            spark.catalog.dropTempView(f"{view}")

        df.cache()

        (
            df.coalesce(1)
            .write.format("parquet")
            .mode("overwrite")
            .save(self.path_curated)
        )

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

    spark.sparkContext.setLogLevel("ERROR")

    m = ServeData(spark)

    m.run()

    spark.stop()