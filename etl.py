from pyspark.sql.types import StringType, StructField, StructType
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import os


class JsonToParquet:
    def __init__(self, json_dir, parquet_path):
        self.schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", StringType(), True),
                StructField("ts", StringType(), True),
            ]
        )
        self.sc = SparkContext.getOrCreate()
        self.spark = SparkSession(self.sc)
        self.json_dir = json_dir
        self.parquet_path = parquet_path
        self.df = self.spark.createDataFrame([], self.schema)
        self.file_names = [f for f in os.listdir(self.json_dir)]

    def import_json(self, import_dir, file_name):
        with open(os.path.join(import_dir, file_name), "r") as f:
            file = json.loads(str(f.read()))
            return file

    def append_json_records(self):
        for f in self.file_names:
            records_dict = self.import_json(self.json_dir, f)
            records = records_dict["records"]
            new_rows = self.spark.createDataFrame(records, self.schema)
            self.df = self.df.union(new_rows)
        return self.df

    def dedup_records(self):
        self.df = self.df.dropDuplicates()
        return self.df

    def write_to_parquet(self):
        self.df.write.mode("overwrite").option("compression", "snappy").parquet(
            self.parquet_path
        )


if __name__ == "__main__":
    json_to_parquet = JsonToParquet(
        json_dir="./json_records", parquet_path='./records.parquet'
    )
    df_raw = json_to_parquet.append_json_records()
    df_dedup = json_to_parquet.dedup_records()
    json_to_parquet.write_to_parquet()
