# Sparkify Data Warehouse

## Introduction
Sparkify is a grower music streaming startup which aims to move their 
data processes and analyses onto the cloud. Their data resides in S3, in 
two directories consisting of a JSON logs from their app users 
activities, as well as a JSON metadata with the songs in the app.

Bellow are two examples of how the JSON files look like.

### Songs

Each song json file contains information about the song and artist such as the title, artist Id, location, name and duration of the song.

```python
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Logs:
Each line of this json brings information about the users (name, gender, location and account payment level), artists, song's title, page in the app and time.
![log dat](/images/log-data.png)


## ETL Pipeline

As a data engineering case, this project has an ETL pipeline that 
extracts their data from S3, processes them using Spark, and loads the 
data back into S3 as a set of dimensional tables. This will allow their 
analytics team to continue finding insights in what songs their users are 
listening to.

## Usage

1. Run
```python
python3 etl.py --key [YOUR_AWS_ACCESS_KEY_ID] --secret [AWS_SECRET_ACCESS_KEY] -from [INPUT_BUCKET] -to [OUTPUT_BUCKET]
```
## Schemas

### **Fact Table**

>- **songplays** - records in log data associated with song plays i.e. records with page NextSong
>    - songplay_id
>    - start_time
>    - user_id
>    - level
>    - song_id
>    - artist_id
>    - session_id
>    - location
>    - user_agent

### **Dimension Tables**

>- **users** - users in the app
>    - user_id
>    - first_name
>    - last_name
>    - gender
>    - level

>- **songs** - songs in the music database
>    - song_id
>    - title
>    - artist_id
>    - year
>    - duration

>- **artists** - artists in the music database
>    - artist_id
>    - name
>    - location
>    - lattitude
>    - longitude

>- **time** - timestamps of records in songplays broken down into specific units
>    - start_time
>    - hour
>    - day
>    - week
>    - month
>    - year
>    - weekday

