#!/usr/bin/env python

"""Tests for `file_streaming_job` package."""
from dataclasses import dataclass
from time import sleep
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row, IntegerType, DoubleType

from file_streaming_job.config import get_config, ApplicationConfig
from file_streaming_job.file_sync import FileSync
from tests._spark_session import get_spark_session

data = [
    ("36636", 3000, 0.1),
    ("Michael", 4000, 0.2),
    ("Robert", 4000, 0.3),
    ("Maria", 4000, 0.4),
    ("Jen", -1, 0.5)
]

base_schema = StructType(
    [
        StructField("COL1", StringType(), True),
        StructField("COL2", IntegerType(), True),
        StructField("COL3", DoubleType(), True)
    ]
)

inferred_schema = StructType(
    [
        StructField("COL1", StringType(), True),
        StructField("COL2", StringType(), True),
        StructField("COL3", StringType(), True),
        StructField("__file_name", StringType(), True)
    ]
)


@dataclass
class Stat:
    dataset: List[Row]
    schema: StructType
    total_rows: int


def prepare_dataset(spark, config):
    df = spark.createDataFrame(data=data, schema=base_schema)

    df.write.csv(config.file.path, header='true')

    print(df.show())


def get_stats_if_ready(spark: SparkSession, test_config: ApplicationConfig):
    try:

        query = f"select * from {test_config.database.schema}.{test_config.database.table}"
        df = spark.sql(query)
        dataset = df.collect()
        print(df.show())
        stat = Stat(
            dataset=dataset,
            schema=df.schema,
            total_rows=len(dataset)
        )

        return stat
    except Exception as e:
        print("Error on get stats: " + e)

    return None


def test_file_sync():
    # create test spark instance
    test_config = get_config("application.conf")
    spark = get_spark_session()
    prepare_dataset(spark, config=test_config)
    # run target
    job = FileSync(spark, test_config)
    job.sync()

    # check
    sleep(10)
    job._stop()
    stat = get_stats_if_ready(spark, test_config)
    assert stat.schema == inferred_schema
    assert stat.total_rows == 5
