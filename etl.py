import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, DateType , LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will initiate a spark session when called
    args: 
        None
    returns: 
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_song_data_schema():
    """
    This function will return the schema definition for song source data 
    args: 
        None
    return: 
        The schema definition for song source data 
    """
    
    song_schema = StructType(
        [
            StructField("num_songs", IntegerType()),
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", DoubleType()),
            StructField("year", IntegerType())
        ]
    )
    
    return song_schema


def get_log_data_schema():
    """
    This function will return the schema definition for log source data 
    args: 
        None
    returns: 
        The schema definition for log source data 
    """
    
    log_schema = StructType(
        [
            StructField("artist", StringType()),
            StructField("auth", StringType()),
            StructField("firstName", StringType()),
            StructField("gender", StringType()),
            StructField("itemInSession", IntegerType()),
            StructField("lastName", StringType()),
            StructField("length", DoubleType()),
            StructField("level", StringType()),
            StructField("location", StringType()),
            StructField("method", StringType()),
            StructField("page", StringType()),
            StructField("registration", StringType()),
            StructField("sessionId", IntegerType()),
            StructField("song", StringType()),
            StructField("status", IntegerType()),
            StructField("ts", LongType()),
            StructField("userAgent", StringType()),
            StructField("userId", StringType())
        ]
    )
    
    return log_schema


def read_song_data(spark, input_data):
    """
    This function will read the song source data from a location 
    args: 
        spark: SparkSession
        input_data: directory (e.g. local directory or S3 bucket directory)
    returns: 
        A dataframe containing songs data from source
    """
    
     # get filepath to song data file
#     song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    song_data = os.path.join(input_data, "song_data/A/B/C/TRABCEI128F424C983.json")
    
    # Get schema for song source data
    song_schema = get_song_data_schema()
    
    # read song data file
    songs_data_df = spark.read.schema(song_schema).json(song_data)
    
    return songs_data_df


def read_log_data(spark, input_data):
    """
    This function will read the song source data from a location 
    args: 
        spark: SparkSession
        input_data: directory (e.g. local directory or S3 bucket directory)
    return: 
        A dataframe containing songs data from source
    """
    
    # get filepath to log data file
#     log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')
    log_data = os.path.join(input_data, 'log_data/2018/11/2018-11-12-events.json')
    
    # Get schema for log source data
    log_schema = get_log_data_schema()
    
    # read log data file
    log_data_df = spark.read.schema(log_schema).json(log_data)
    
    return log_data_df


def process_song_data(spark, input_data, output_data):
    """
    This function will process the song data from source, into FACT and DIMENSION tables
    and then load to a destination
    args: 
        spark: SparkSession
        input_data: directory (e.g. local directory or S3 bucket directory)
        output_data: directory (e.g. local directory or S3 bucket directory)
    return: 
        None
    """
    
    songs_data_df = read_song_data(spark, input_data)

    # extract columns to create songs table
    songs_table = (
        songs_data_df.select(
            ['song_id', 'title', 'artist_id', 'year', 'duration']
        ).dropDuplicates(["song_id"])
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        os.path.join(output_data, 'songs'), 
        partitionBy = ['year', 'artist_id'],
        mode="overwrite"
    )

    # extract columns to create artists table
    artists_table = (
        songs_data_df.select(
            ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
        ).withColumnRenamed('artist_name', 'name')
        .withColumnRenamed('artist_location','location')
        .withColumnRenamed('artist_latitude','lattitude')
        .withColumnRenamed('artist_longitude','longitude')
        .dropDuplicates(["artist_id"])
    )

    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, 'artists'),
        mode="overwrite"
    )


def process_log_data(spark, input_data, output_data):
    """
    This function will process the log data from source, into FACT and DIMENSION tables
    and then load to a destination
    args: 
        spark: SparkSession
        input_data: directory (e.g. local directory or S3 bucket directory)
        output_data: directory (e.g. local directory or S3 bucket directory)
    return: 
        None
    """
    
    log_data_df = read_log_data(spark, input_data)
    
    # filter by actions for song plays
    log_data_NextSong_df = (
        log_data_df.where(
            log_data_df.page == 'NextSong'
        )
    )

    # extract columns for users table    
    user_table = (
        log_data_NextSong_df.select(
            ['userId', 'firstName', 'lastName', 'gender', 'level']
        ).withColumnRenamed('userId','user_id')
        .withColumnRenamed('firstName','first_name')
        .withColumnRenamed('lastName','last_name')
        .dropDuplicates(["user_id"])
    )
    
    # write users table to parquet files
    user_table.write.parquet(
        os.path.join(output_data, 'users'),
        mode="overwrite"
    )

    # create timestamp and datetime column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), TimestampType())
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), DateType())
    
    log_data_df = (
        log_data_df.withColumn(
            "timestamp", get_timestamp(col("ts"))
        )
        .withColumn(
            "datetime", get_timestamp(col("ts"))
        )
    )
    
    # extract columns to create time table
    time_table = (
        log_data_df.select('timestamp')
        .withColumnRenamed('timestamp', 'start_time')
        .withColumn('hour', hour('start_time'))
        .withColumn('day', dayofmonth('start_time'))
        .withColumn('week', weekofyear('start_time'))
        .withColumn('month', month('start_time'))
        .withColumn('year', year('start_time'))
        .withColumn('weekday', dayofweek('start_time'))
        .dropDuplicates(["start_time"])
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        os.path.join(output_data, 'time'),
        partitionBy = ["year", "month"],
        mode="overwrite"
    )

    # read in song data to use for songplays table
    songs_data_df = read_song_data(spark, input_data)
    
    songs_songplays_df = (
        songs_data_df.select(
            ['song_id', 'artist_id', 'title', 'duration', 'artist_name']
        ).dropDuplicates(['song_id', 'artist_id'])
    )
    
    #read in log data to use for songplays
    log_songsplay_df =(
        log_data_df.select(
            ['timestamp', 'userId', 'level', 'sessionId', 'location', 'userAgent','song', 'length', 'artist']
        ).withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
        .withColumnRenamed("timestamp", "start_time")
        .withColumnRenamed("userId", 'user_id')
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
    )
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        log_songsplay_df.join(songs_songplays_df,
                  on = ((log_songsplay_df.song == songs_songplays_df.title) & (log_songsplay_df.length == songs_songplays_df.duration) & (log_songsplay_df.artist == songs_songplays_df.artist_name)),
                  how = "left"
                 )
        .withColumn('songplay_id', monotonically_increasing_id())
        .select(
            ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', "year", "month"]
        )
    )
            

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        os.path.join(output_data, 'songplays'),
        partitionBy = ["year", "month"],
        mode="overwrite"
    )


def main():
    """
    This function will run the pipeline when executing this file
    args:
        None
    returns:
        None
    """
    # Initiate Spark Session
    spark = create_spark_session()
    
    # Input and output directories
    input_data = "s3a://udacity-dend/"
    output_data = "./data/"
#     output_data = "s3a://spark-s3-udacity/data/"
    
    # Process data
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
