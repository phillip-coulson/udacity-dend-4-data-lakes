# Sparkify Data Lake Pipeline

Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data currently resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The business requies an ETL pipeline to extracts their data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# PreRequisits

Before running the ETL pipeline, the user must provide AWS credentials for an S3 Role with the correct Read and Write permissions:

- **AWS Key**
- **AWS Secret Key**

# Running the pipeline

The pipeline can be run from a terminal by executing the below in the root directory
' python etl.py'

This will initialise a SparkSession, read the JSON data from S3, create the data for the destination dimension tables and write back to an S3 Bucket.

# Source Data

There are two data sources that reside in S3. Here are the S3 links for each:

Song data: s3a://udacity-dend/song_data
Log data: s3a://udacity-dend/log_data

## Song Data

This is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

'song_data/A/B/C/TRABCEI128F424C983.json'
'song_data/A/A/B/TRAABJL12903CDCF1A.json'

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

'{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, "year": 0
 }'
 
 ## Log Data
 
The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

'log_data/2018/11/2018-11-12-events.json'
'log_data/2018/11/2018-11-13-events.json'

![log-data-example](./log-data.png?raw=true "Log Data Example")

# Data Schema

The below STAR Schema data model has been used:
FACT table: **songplays**
DIMENSION tables: **users, artists, time, songs**

![Data Model](./data_model-data.png?raw=true "Data Model")