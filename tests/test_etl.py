from etl import JsonToParquet
import os
from unittest import TestCase


class TestJsonToParquet(TestCase):
    def setUp(self):
        self.json_dir = "tests/data"
        self.parquet_dir = "tests/parquet_files"
        self.json_to_parquet = JsonToParquet(
            json_dir=self.json_dir, parquet_dir=self.parquet_dir
        )
        self.json_files_array = [f for f in os.listdir(self.json_dir)]

    def test_import_json(self):
        json_dir = "tests/data"
        file_types = [f.split(".")[1] for f in os.listdir(json_dir)]
        assert file_types == ["json"], "Non-json files imported from JSON directory."
        assert os.path.exists(json_dir), "Directory not found."
        assert len(self.json_files_array) > 0, "Json file directory is empty."
        assert [f.split(".")[0] for f in self.json_files_array] == [
            "records"
        ], "Json file names don't match."

    def test_load_json(self):
        df = self.json_to_parquet.load_json()
        parquet_files = [f for f in os.listdir(self.parquet_dir)]
        assert len(parquet_files) > 0, "Parquet files directory is empty."
        assert df.count() == 3, "There should be three records in the dataframe."
        assert [list(row) for row in df.collect()] == [
            ["10102", "CAT", "2019-10-23T18:20:32.876Z"],
            ["10130", "DOG", "2019-08-23T17:25:43.111Z"],
            ["10120", "BIRD", "2019-05-23T18:20:32.876Z"],
        ], "Dataframe output is incorrect."
