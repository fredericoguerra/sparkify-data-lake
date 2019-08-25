import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song-data/A/A/A/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where('page = "NextSong"')

    # extract columns for users table    
    users_table = df.select(['userId','firstName','lastName','gender','level'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(ts/1000.0)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: str(datetime.fromtimestamp(int(ts)/1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),\
    hour('datetime').alias('hour'),\
    dayofmonth('datetime').alias('day'),\
    weekofyear('datetime').alias('week'),\
    month('datetime').alias('month'),\
    year('datetime').alias('year')).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_data = input_data + "song-data/A/A/A/*"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df.join(song_df, (song_df.title == df.song))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_df.select(
col('ts').alias('start_time'),
col('userId').alias('user_id'),
col('level').alias('level'),
col('song_id').alias('song_id'),
col('artist_id').alias('artist_id'),
col('sessionId').alias('session_id'),
col('artist_location').alias('location'),
col('userAgent').alias('user_agent'),
year('datetime').alias('year'),
month('datetime').alias('month')
)

    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'songplays')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://fguer-sparkify-v2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
