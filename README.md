## Date created
Date : 12-07-2019

## Project Title
Data Lakes

## Description
In this project, we will apply the concept of *Spark and Data Lakes to build an ETL pipeline for a data lake hosted on S3*.

A music streaming startup, **Sparkify**, has grown their user base and song database even more and want to move their data warehouse to a **data lake**. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Summary
* The purpose of this project is to accumulate their data into more flexible storage than data warehouse.
* These data need to be store in Data Lakes using Spark
* As their data engineer, our task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional
  tables.
* This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Files used
### Python Files:
* etl.py - Reads data from S3, processes that data using Spark, and writes them back to S3

### Config File:
* dl.cfg - Contains the AWS access keys and AWS secret access keys

### Project Steps
* Read the data from S3
* Process that data using Spark
* Write the processed back to S3


## Credits
http://millionsongdataset.com/

