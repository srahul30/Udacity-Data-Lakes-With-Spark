# Importing Libraries as Required
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Setting Configurations to Access S3 buckets from my account. 
# In the Configurtions file header was not present hence added the header as AWS
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''
    Function Defination - This function is to create the Spark Session and launch hadoop
    Parameters - None
    Return - It returns the spark connection to the calling function
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    Function Defination - This function processes the song file retrived from Udacity S3 Bucket and stores the songs and artist    dimensional tables in my individual S3 Bucket
    Parameters - Spark Connection, S3 Bucket link for Udacity, S3 Bucket link for my AWS account respectively
    Return - None
    '''
    # get filepath to song data file
    # Getting only the file from path "song-data/A/A/A/*.json" as processing full file is taking more than 3-4 hours
    song_data = os.path.join(input_data,"song-data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # Printing songs df Schema
    print('SONGS Dataframe Schema')
    df.printSchema()

    # extract columns to create songs table
    songs_table = spark.sql('''
                            SELECT distinct song_id, 
                                            title, 
                                            artist_id, 
                                            year, 
                                            duration 
                            FROM   songs
                            WHERE  song_id IS NOT NULL
                            ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")
    
    print('Song Table Uploaded')

    # extract columns to create artists table
    artists_table = spark.sql('''
                              SELECT distinct artist_id, 
                                              artist_name, 
                                              artist_location, 
                                              artist_latitude, 
                                              artist_longitude 
                              FROM   songs
                              WHERE  artist_id IS NOT NULL
                              ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")
    
    print('Artists Table Uploaded')
    
def process_log_data(spark, input_data, output_data):
    '''
    Function Defination - This function processes the log file retrived from Udacity S3 Bucket and stores the user and time dimensional tables in my individual S3 Bucket
    Parameters - Spark Connection, S3 Bucket link for Udacity, S3 Bucket link for my AWS account respectively
    Return - None
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # Creating Temporary table
    df.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    df = spark.sql('''
                   SELECT * 
                   FROM staging_events 
                   WHERE page = 'NextSong'
                   ''')
    
    print('Staging events Table Filtered')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # Creating Temporary table after filtering for song plays
    df.createOrReplaceTempView("staging_events")
    
    # Printing final Staging Events/Logs df Schema
    print('LOGS Dataframe Schema')
    df.printSchema()
    
    # extract columns for users table    
    users_table = spark.sql('''
                            SELECT  DISTINCT userId,
                                    firstName,
                                    lastName,
                                    gender
                            FROM    staging_events
                            WHERE   userId IS NOT NULL
                            ''')
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")
    
    print('User Table Uploaded')
    
    # extract columns to create time table
    time_table = spark.sql('''
                           SELECT distinct timestamp as start_time, 
                                  hour(timestamp) as hour, 
                                  day(timestamp) as day, 
                                  weekofyear(timestamp) as week, 
                                  month(timestamp) as month, 
                                  year(timestamp) as year, 
                                  weekday(timestamp) as weekday
                           FROM   staging_events
                           ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time.parquet", mode = "overwrite")
    
    print('Time Table Uploaded')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs_parquet")
    
    print('Songs Parquet Downloaded from my S3')
    
    # Printing songs_parquet df Schema from my S3 Bucket
    print('SONGS_PARQUET Dataframe Schema')
    song_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT a.timestamp as start_time, 
                                       a.userId, 
                                       a.level, 
                                       b.song_id, 
                                       b.artist_id, 
                                       a.sessionId, 
                                       a.location, 
                                       a.userAgent, 
                                       year(a.timestamp) as year, 
                                       month(a.timestamp) as month 
                                FROM staging_events as a 
                                inner join songs_parquet as b on a.song = b.title
                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "/songplays/songplays.parquet", mode = "overwrite")
    print('Fact Table is Uploaded')
    
    
def main():
    '''
    Function Defination - This is the main function and is used to call multiple functions below -
        1. create_spark_session() - for creating spark session
        2. process_song_data(spark, input_data, output_data) - To Process song file from Udacity S3 Bucket 
        3. process_log_data(spark, input_data, output_data) - To Process log file from Udacity S3 Bucket 
    Parameters - None
    Return - None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rahul-data-lake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
