import os
from dataclasses import dataclass

from pyhocon import ConfigFactory

checkpointLocation = "file-streaming/data/_checkpoints_" + os.getenv("SPARK_INSTANCE_ID")


@dataclass
class FileConfig:
    format: str
    path: str
    max_files_per_trigger: str
    latest_first: str
    max_file_age: str


@dataclass
class DbConfig:
    schema: str
    table: str


@dataclass
class ApplicationConfig:
    file: FileConfig
    database: DbConfig
    processing_time: str
    checkpoint_location: str


def format_processing_time(interval, unit):
    return "{} {}".format(interval, unit)


def get_config(application_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_path)

    file = FileConfig(
        format=config['file']['format'],
        path=config['file']['path'],
        max_files_per_trigger=config['file']['max_files_per_trigger'],
        latest_first=config['file']['latest_first'],
        max_file_age=config['file']['max_file_age']
    )
    database = DbConfig(
        schema=config['database']['schema'],
        table=config['database']['table']
    )
    processing_time = format_processing_time(config['processing_time']['interval'],
                                             config['processing_time']['unit'])
    return ApplicationConfig(
        file=file,
        database=database,
        processing_time=processing_time,
        checkpoint_location=checkpointLocation,
    )
