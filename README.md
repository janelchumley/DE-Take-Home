### Navis Data Engineer Coding Assessment

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

#### Solution 
