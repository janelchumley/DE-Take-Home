from pyspark.sql.types import StringType, StructField, StructType
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import os


class JsonToParquet:
    """Setting up global variables"""

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
        self.json_files = [f for f in os.listdir(self.json_dir) if f.endswith(".json")]
        self.parquet_path = parquet_path
        self.df = self.spark.createDataFrame([], self.schema)

    """This method imports a json file from a given directory."""

    def import_json(self, import_dir, file_name):
        with open(os.path.join(import_dir, file_name), "r") as f:
            file = json.loads(str(f.read()))
            return file

    """ This method iterates through all of the json files in a given directory,
        creates a new dataframe containing those new records,
        and appends the new records to the global dataframe 
        using the Pyspark union operator/method."""

    def append_json_records(self):
        for f in self.json_files:
            records_dict = self.import_json(self.json_dir, f)
            records = records_dict["records"]
            new_rows = self.spark.createDataFrame(records, self.schema)
            self.df = self.df.union(new_rows)
        return self.df

    """This method calls the Spark dropDuplicates 
        method on the global dataframe."""

    def dedup_records(self):
        self.df = self.df.dropDuplicates(["id", "ts"])
        return self.df

    """After new records are added to the global dataframe and
       duplicates are eliminated, the data is written to the parquet file.
       The 'overwrite' mode parameter is used to avoid duplicates."""

    def write_to_parquet(self):
        self.df.write.mode("overwrite").option("compression", "snappy").parquet(
            self.parquet_path
        )


if __name__ == "__main__":
    json_to_parquet = JsonToParquet(
        json_dir="./json_records", parquet_path="./records.parquet"
    )
    df_raw = json_to_parquet.append_json_records()
    df_dedup = json_to_parquet.dedup_records()
    json_to_parquet.write_to_parquet()
