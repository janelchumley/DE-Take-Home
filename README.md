### Data Engineer Coding Assessment

#### Problem statement 
You have a list of records in json format like the following sample:
    
```{
“records” : [
                {
                                “id”:”10101”,
                                “data”:”ABC”,
                                “ts”:”2019-04-23T17:25:43.111Z”
                },
                {
                                “id”:”10102”,
                                “data”:”XYZ”,
                                “ts”:”2019-04-23T18:20:32.876Z”                            
                }
]
}
```


You are constantly reading files with this format and you must persist the records in parquet format.

But we don’t want to duplicate records. If one file is duplicated and both are been read, your python code should avoid the duplication. 

Duplicate records would be something with the same id and same ts even if data field is different. We may have a lot of records with the same id but all the ts must be unique.

Please build a ETL python program that will read all the json files in a filesystem path and create parquet files that will avoid duplication.

The solution should be high performant and should include unit tests.

### Getting Started 
1. Install Pyspark
    * The following link provides a tutorial on setting up Pyspark locally: https://sharing.luminis.eu/blog/how-to-install-pyspark-and-apache-spark-on-macos/

2. Create a virtual environment 

    ```python3 -m venv navisenv```
3. Activate the virtual environment

    ```source activate navisenv```
    
4. Install dependencies via pip 

    ```pip install -r requirements.txt```
    
5. Run etl.py

    ```python etl.py```
6. Run test_etl.py

    ```python tests/test_etl.py```
 


