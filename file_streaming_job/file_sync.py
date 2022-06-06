import logging

from pyspark.sql import SparkSession, functions

logger = logging.getLogger(__name__)


class FileSync:
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.streaming_query = None

    def sync(self):
        logger.info(f"sync stating for source path={self.config.file.path}")
        df = self.spark.readStream \
            .format(self.config.file.format) \
            .option("header", "true") \
            .option("path", self.config.file.path) \
            .option("maxFilesPerTrigger", self.config.file.max_files_per_trigger) \
            .option("latestFirst", self.config.file.latest_first) \
            .option("maxFileAge", self.config.file.max_file_age) \
            .option("fileNameOnly", "false") \
            .load() \
            .withColumn("__file_name", functions.input_file_name())

        self.streaming_query = df.writeStream \
            .foreachBatch(self.__foreach_batch_sync) \
            .trigger(processingTime=self.config.processing_time) \
            .option("checkpointLocation", self.config.checkpoint_location) \
            .start() \
            .awaitTermination()  # remove this for testing

    def __foreach_batch_sync(self, df, epoch_id):
        """
        Responsible for processing micro batches for every batch processing.;

        :param df: Batch dataframe to be written.;
        :param epoch_id: Micro batch epoch id.;
        """
        logger.debug(f"foreach_batch_sync start epoch_id = {epoch_id}, " +
                     f"database_table_name = {self.config.database.table}")
        try:
            df.write.saveAsTable(
                self.__complete_db_destination(self.config.database.schema, self.config.database.table),
                format='iceberg',
                mode='append')
        except Exception as e:
            logger.error(f"error stream processing table = {self.config.database.table}")
            logger.error(e)

    @staticmethod
    def __complete_db_destination(schema, table):
        return "{}.{}".format(schema, table)

    def _stop(self):
        logger.info("stopping...")
        self.streaming_query.stop()
        logger.info("stopped")
