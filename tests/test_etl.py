from etl import JsonToParquet
import os
from unittest import TestCase
from pyspark.sql import SparkSession


class TestJsonToParquet(TestCase):
    def setUp(self):
        self.json_dir = "tests/data"
        self.json_to_parquet = JsonToParquet(
            json_dir=self.json_dir, parquet_path="tests/test.parquet"
        )
        self.schema = self.json_to_parquet.schema
        self.sc = self.json_to_parquet.sc
        self.spark = SparkSession(self.sc)
        self.raw_df = self.json_to_parquet.append_json_records()
        self.raw_df_array = [list(row) for row in self.raw_df.collect()]
        self.dedup_df = self.json_to_parquet.dedup_records()
        self.dedup_df_array = [list(row) for row in self.dedup_df.collect()]
        self.json_to_parquet.write_to_parquet()

    def test_import_json(self):
        json_files_array = [f for f in os.listdir(self.json_dir)]
        file_types_array = [f.split(".")[1] for f in json_files_array]
        file_names_array = [f.split(".")[0] for f in json_files_array]
        assert os.path.exists(self.json_dir), "Directory not found."
        assert len(json_files_array) > 0, "Json file directory is empty."
        assert file_types_array == [
            "json",
            "json",
        ], "Non-json files imported from JSON directory."
        assert sorted(file_names_array) == [
            "records1",
            "records2",
        ], "File name doesn't match expected output."

    def test_append_json_records(self):
        assert (
            self.raw_df.count() == 7
        ), "There should be 7 records in the raw dataframe."
        assert self.raw_df_array == [
            ["10130", "DOG", "2019-08-23T17:25:43.111Z"],
            ["10102", "CAT", "2019-10-23T18:20:32.876Z"],
            ["10120", "BIRD", "2019-05-23T18:20:32.876Z"],
            ["10130", "DOG", "2019-08-23T17:25:43.111Z"],
            ["10102", "CAT", "2019-10-23T18:20:32.876Z"],
            ["10130", "DOG", "2019-08-23T17:25:43.111Z"],
            ["10102", "CAT", "2019-10-23T18:20:32.876Z"],
        ], "Raw dataframe output is incorrect."

    def test_dedup_records(self):
        assert (
            self.raw_df_array != self.dedup_df_array
        ), "Raw and dedup dataframe arrays have matching output."
        assert (
            self.dedup_df.count() == 3
        ), "There should be 3 records in the dedup dataframe."
        assert self.dedup_df_array == [
            ["10102", "CAT", "2019-10-23T18:20:32.876Z"],
            ["10130", "DOG", "2019-08-23T17:25:43.111Z"],
            ["10120", "BIRD", "2019-05-23T18:20:32.876Z"],
        ], "Dedup dataframe output is incorrect."

    def test_write_to_parquet(self):
        parquet_df = self.spark.read.parquet("tests/test.parquet")
        parquet_df_array = sorted([list(row) for row in parquet_df.collect()])
        assert len(parquet_df_array) > 0, "There are no parquet files."
        assert (
            parquet_df.count() == self.dedup_df.count()
        ), "The parquet and dedup dataframes should have the same number of records."
        assert parquet_df_array == sorted(
            self.dedup_df_array
        ), "Parquet and dedup dataframes should have matching output."
