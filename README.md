# TLC Yellow-Taxi

The purpose of the code can be found under Tasks.

## Docs

### Requirements:
- mysql connection;


### Contents:
```
        - run_everything.sh: for a quick setup, insert required variables into the script and run: sh run_everything.sh 
                In theory it should work, in practice it may enouncter errors due to a different os or other. (prerequisites: osx-arm64 platform and conda)
                In case of error, the environment should be set up manually according to the spec-file.txt.
        - spec-file.txt: used for setting up the environment;
        - split.py: splits the monthly parquet files;
        - setup.py: for setting up the relevant DB;
        - day_update.ipynb: updates DB with data stored in one file;
        - month_update.py: updates DB with data stored in a folder;
```

## Tasks: 

1. Imagine that you receive data in single day batches, based on the field “tpep_dropoff_datetime”. Split the parquet file into multiple files based on this field. You should end up in roughly 30 files. 

2. Your next task is to implement a process that loads all of the files into the database. It would be beneficial that the implementation is reusable, i.e. can be used for the future data files as well. 

3. Test the quality of the data. What metrics would you use to monitor the quality of the data? Are there any anomalous or suspicious data points in the data? We would like to collect data quality metrics of processed data each time it is loaded into the database. Could you implement this functionality and store such metrics in a separate database table or any other place that you seem fit?

4. Model the dataset to represent a fact-dimension type of schema. Draw the entity relationship diagram of your model and transform the data to fit it.

5. Can you identify the weak points of the proposed solution? If you had more time, what parts of your solution would you improve? 

## Answers:

1. Code:  ```split.py```. Files: ```data/2020/01```
2. (and 3) 
- code: can be found in ```day_update.ipynb```, which includes a more thorough comparison of raw data to clean data or ```month_update.py``` to load a whole month.
- the stored metrics (see ```metrics_2020.csv```) include: 
```     
       - date: year-month-day of the file;,
       - non_error_ratio: raw to clean data ratio,
       - empty_values_ratio: missing values to non-missing values ratio,
       - missing_values: missing value count,
       - nan_rows_without_airport_fee: NaN row count excluding airport_fee,
       - data_usage: how much data will be loaded to the db,
       - data_reduction_after_cleanup: how much data was reduced during clean-up,
       - duplicates_ratio: making sure no duplicates stay in the data (based on unique id),
       - processing_time.
```
- the metrics in the reports (found in the notebook only) also show unique, missing, min and max values per row.
- anomalous and suspicious data points: 
```
        - there is unique 3 vendor_id values, although only 2 are specified in the TLC documentation;
        - airport_fee contains only NaNs, so deleted; 
        - some NaN value rows in columnspassenger_count,rate_code_id,congestion_surcharge, so deleted;
        - it is unclear how some of the amount columns can have minus dollars, but left as is;
        - there are 0 miles distances and 0 passenger count distances, but left as is;
```


4. See: ```diagram.png```
5. 
__Weak points__:
- under metrics, not sure if ```rows_with_nan``` is necessary; is is also the part of the code which is slowing down the processing;
- no metrics gathered for the suspicious data points (the minus amounts or 0 distance, passenger count trips);


__Would improve__:
- the suspicious data points left in the data as-is, would propose a solution;
- add more business-oriented data quality metrics;
- create a separate database to store not only the metrics (now they are stored in a csv), but as full reports for each day/aggregated per month.
