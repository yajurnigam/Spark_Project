import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

class Pipeline:
    def __init__(self):
        pass
    def run_pipeline(self):
        print("Running Pipeline")
        transform=Transform()
        transform.transform_data()
        persist=Persist()
        persist.persist_data()

class Ingest:

    def __init__(self,spark):
        self.spark=spark

    def ingest_data(self):
        print("Ingesting 123")
        my_list = [1, 2, 3]
        df = self.spark.createDataFrame(my_list, IntegerType())
        df.show()
class Transform:
    def __init__(self,spark):
        self.spark=spark
    def transform_data():
        print("Transforming")

class Persist:

    def __init__(self,spark):
        self.spark=spark

    def persist_data(self):
        print("Persisiting")
if __name__=='__main__':
    pipeline=Pipeline()
    pipeline.run_pipeline()