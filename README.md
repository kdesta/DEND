
## The Requirement
Business for Sparkify has boomed and its  data grew fast and has been arriving  in variety of forms that its current data warehouse model  can no more handle.  Data is now handled in a datalake (refer to definition elsewhere). The company’s IT team is tasked with answering business questions from data that has been loaded into a datalake.
## Solution
Build an ETL pipeline that extracts data from S3, process them using Spark Data Processing Engine to s3. From the S3, analytics team can perform analysis and develop insight for making business decision.

## Activities

### Test data ETL pipeline 
To ensure the development of a functional ETL pipeline on a big data, a small data set was used with pyspark and other necessary packages (APIs). The test.ipnynb file contains the script for this purpose with brief in-line explanation of steps.
### AWS S3 datalake ETL pipeline tasks
1. Accessed  and read Log and Song data that reside in S3. Song data: s3://udacity-dend/song_data Log data: s3://udacity-dend/log_data
2. Launched an EMR cluster on AWS side that had an IAM role that has full access to S3 to read the data
3. Performed transformation using spark and results were written in parquet files in a separate analytics directory on S3.
4. Verified data being transformed using Spar- SQL Functions by creating Temporary Views for each of these tables in etl.py file.
5. Fact and dimension tables are the same as previous projects.
