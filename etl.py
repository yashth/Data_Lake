import configparser
import pandas as pd
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
import pyspark.sql.functions as F
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Ti, LongType as Lo, TimestampType as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Schema for songs data
song_table_schema = R([
    Fld('artist_id',Str()),
    Fld('artist_latitude',Dbl()),
    Fld('artist_location',Str()),
    Fld('artist_longitude',Dbl()),
    Fld('artist_name',Str()),
    Fld('duration',Dbl()),
    Fld('num_songs',Dbl()),
    Fld('song_id',Str()),
    Fld('title',Str()),
    Fld('year',Int()),
])

#Create Spark Session
def create_spark_session():
    """
    Create Spark Session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Song Data Process using Spark
def process_song_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    
    # File path for song data 
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # Path to write the data back to S3
    output_songs_data = os.path.join(output_data,"songs_table")
    output_artists_data = os.path.join(output_data,"artists_table")
    
    
    # Read song data file
    df_song_data = spark.read.json(song_data,schema=song_table_schema).dropDuplicates()
    df_song_data.printSchema()
    df_song_data.show(5)

    # Extract columns for songs table
    songs_table = df_song_data.selectExpr("song_id","title","artist_name","artist_id","year","duration")
    songs_table.printSchema()
    songs_table.show(5)
    
    # Write songs table back to S3
    songs_table.partitionBy("year", "artist_id").write.parquet(output_songs_data, mode="overwrite")

    # Extract columns for artists table
    artists_table = df_song_data.selectExpr("artist_id", 
                                            "artist_name as name", 
                                            "artist_location as location", 
                                            "artist_latitude as latitude", 
                                            "artist_longitude as longitude")
    artists_table.printSchema()
    artists_table.show(5)
    
    # Write artists table back to S3
    artists_table.write.parquet(output_artists_data, mode="overwrite")


# Log Data Process Using Spark
def process_log_data(spark, input_data, output_data):
    """
    Read log data and process it and sand create songplays table to 
    using both the log data and song data
    Store the songplays data to specified output location
    
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    output_users_data = os.path.join(output_data,"users_table")
    output_time_data = os.path.join(output_data,"time_table")
    output_songs_data = os.path.join(output_data,"songs_table")
    output_songplays_data = os.path.join(output_data,"songplays_table")
    
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")
    print(log_data)
    
    # read log data file
    df_log_data = spark.read.json(log_data).dropDuplicates()
    df_log_data.printSchema()
    df_log_data.show(5)
    
    # filter by actions for song plays
    df_log_data = df_log_data.filter("page=='NextSong'")
    df_log_data.show(5)
    
    
    # extract columns for users table    
    users_table = df_log_data.selectExpr("userId as user_id",
                                         "firstName as first_name",
                                         "lastName as last_name",
                                         "gender",
                                         "level")
    users_table.printSchema()
    users_table.show(5)
    
    # write users table to parquet files
    users_table.write.parquet(output_users_data, mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x : datetime.fromtimestamp(x),T())
    df_log_data = df_log_data.withColumn("start_time",get_timestamp(df_log_data['ts']/1000))
    df_log_data.printSchema()
    df_log_data.show(5)
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x : datetime.fromtimestamp(x),T())
    df_log_data = df_log_data.withColumn("year",year(get_datetime(df_log_data['ts']/1000))) 
    df_log_data = df_log_data.withColumn("month",month(get_datetime(df_log_data['ts']/1000))) 
    df_log_data = df_log_data.withColumn("day",dayofmonth(get_datetime(df_log_data['ts']/1000))) 
    df_log_data = df_log_data.withColumn("hour",hour(get_datetime(df_log_data['ts']/1000))) 
    df_log_data = df_log_data.withColumn("week",weekofyear(get_datetime(df_log_data['ts']/1000)))
    df_log_data = df_log_data.withColumn("weekday",date_format(df_log_data['start_time'],'EEEE'))
    df_log_data.printSchema()
    df_log_data.show(5)
    
    # extract columns to create time table
    time_table =  df_log_data.selectExpr("start_time","hour","day","week","month","year","weekday")
    time_table.printSchema()
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    time_table.partitionBy('year','month').write.parquet(output_time_data, mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_songs_data).dropDuplicates()
    song_df.printSchema()
    song_df.show(5)
    song_df.createOrReplaceTempView("songView")
    
    df_log_data.createOrReplaceTempView("logView")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                        SELECT l.start_time,
                               l.userId as user_id,
                               l.level,s.song_id,
                               s.artist_id,
                               l.sessionId as session_id,
                               l.location,
                               l.userAgent as user_agent,
                               l.year,
                               l.month
                        FROM songView s 
                        JOIN logView l 
                        ON (s.artist_name == l.artist)
    """)
    songplays_table.printSchema()
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.partitionBy("year","month").write.parquet(output_songplays_data, mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
