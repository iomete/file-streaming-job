from pyspark.sql import SparkSession

from file_streaming_job.main import start_job
from file_streaming_job.config import get_config

spark = SparkSession.builder \
    .appName("Job1") \
    .getOrCreate()

production_config = get_config("/etc/configs/application.conf")

start_job(spark, production_config)
