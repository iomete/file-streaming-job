"""Main module."""

from pyspark.sql import SparkSession

from file_streaming_job.file_sync import FileSync
from file_streaming_job.iomete_logger import init_logger


def start_job(spark: SparkSession, config):
    init_logger()
    job = FileSync(spark, config)
    job.sync()

