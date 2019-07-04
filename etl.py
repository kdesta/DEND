import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]=config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """ 
    This script creates or returns existing spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This method defines filepath, input song data file, extract pertinant columns
    for songs and artist tables, and write tables to parquet
    parameters:
        spark: Spark Session connection.
        input data : Udacity S3 Data Lake
        output data : User created S3 Data Lake.    
    Returns:
       None 
    """
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year","artist_id").parquet("s3a://datalake/songs/","overwrite")
    # ============================================================  check=====================================
    # extract columns to create artists table
    ar_listcols= ["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]
    artists_table = song_df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet("s3a://datalake/artists/","overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This code method define filepath, input log data file, extract pertinant columns
    for users, time and songplays tables and write tables to parquet
    """
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    Fil_df = Fil_df.filter(df.page == 'NextSong').filter(df.userId.isNotNull())

    # extract columns for users table    
    users_table = Fil_df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet("s3a://datalake/users/", "overwrite")


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date_time',get_timestamp(df.ts))
    df.createOrReplaceTempView("stage_logs")
    
       
    # extract columns to create time table
    time_table = df.selectExpr(
               "date_time as start_time",\
              "hour(date_time) as hour", \
              "dayofmonth(date_time) as day",\
              "weekofyear(date_time) as week",\
              "month(date_time) as month",\
              "year(date_time) as year",\
              "date_format(date_time,'u') as weekday").dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet("s3a://datalake/time/", "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    df.createOrReplaceTempView("stage_songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =spark.sql(
    """
        SELECT distinct l.ts as start_time,
               l.userId as user_id,
               l.level as level,
               s.song_id as song_id,
               s.artist_id as artist_id,
               l.sessionId as session_id,
               l.location as location,
               l.userAgent as user_agent,
               year(date_time) as year,
               month(date_time) as month
               FROM stage_logs l join stage_songs
               ON (s.artist_name = l.artist and s.title=l.song and s.duration = l.length)
    """)
    
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id()) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet("s3a://datalake/songplays/", "overwrite")


def main():
    """Main method Creates a Spark Session from Pyspark API, Executes the process_song_data and process_log_data functions using parameters provided.
    Paramaters: 
        None    
        Returns:
         None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
