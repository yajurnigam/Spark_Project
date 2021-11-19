import pyspark
from pyspark.sql import SparkSession
import sys
import logging
import logging.config
class Persist:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def persist_data(self,df):
        try:
            logger = logging.getLogger("Persist")
            logger.info('Persisting')
            df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")
        except Exception as exp:
            logger.error("An error occured while persisiting data >"+str(exp))
            # store in database table
            # send an email notification
            raise Exception("HDFS directory already exists")
