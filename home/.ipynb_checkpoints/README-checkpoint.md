# Project Summary
Create an AWS Redshift data warehouse optimized to support song play analysis including the creation of the a database schema and ETL pipeline for the Sparkify analytics team. The included python scripts create a star schema data warehouse of song plays for analysis purposes. The data is from http://millionsongdataset.com/ and is in JSON format.

The database schema for the sparkifydb database ...
Fact Table
- songplays - records in log data associated with song plays
    - songplay_id (primary key)
    - start_time foreign key time table
    - user_id foreign key time table
    - level
    - song_id foreign key
    - artist_id foreign key
    - session_id
    - location
    - user_agent

Dimension Tables
- users - Users in the app. Over time the user's level can change. Current design is to use the most recent user record. A recommended modification would be to use begin and end dates for behavioral analysis purposes. In other words, to understand how a given user's usage changes depending on their level, we would need the dates each level began and ended.
    - user_id (primary key)
    - first_name
    - last_name
    - gender
    - level

- songs - songs in music database
    - song_id (primary key)
    - title
    - artist_id foreign key artist table
    - year
    - duration

- artists - Artists in music database. An individual artist can have multiple names (e.g. Linkin Park, Linkin Park featuring Steve Akoi). The code is designed to select the most common or frequent version. I.e. Linkin Park, not Link Park featuring Steve Akoi). Ideally the upsteam system would generate separate artist ids to represent Linkin Park and Linkin Park featuring Steve Akoi.
    - artist_id (primary key)
    - name
    - location
    - lattitude
    - longitude

- time - Timestamps of records in songplays broken down into specific units. Include a timestamp field for those analyts perferring to use timestamps.
    - start_time (primary key)
    - time_stamp
    - hour
    - day
    - week
    - month
    - year
    - weekday
    
# Explanation of Files in Repository

## sql_queries.py
Contains the create, insert, and drop SQL statements for the above data schema.

## create_tables.py
Contains routines to creates the database, creates the tables, and drop the tables.

## etl.py
Contains the routines to process the JSON files and insert the data into the tables.

## dwh.cfg
Configuration file for AWS. Replace with your own values. Everything else you can use out of the box.

# Explanation of data (stored on AWS S3)

### song data
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### log data
![Image of Log](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)


# Process to Follow

To build your database named sparkifydb (userid = student and password = student) for songplay analysis ...

1. Update configuration file with your own AWS information 
2. execute the command python create_tables.py
3. execute the command python etl.py

Once both scripts complete, the database is ready for querying, thus enabling the end user to performance analysis on song plays.
