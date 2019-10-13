import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    iteminSession INT,
    lastName TEXT,
    length DOUBLE PRECISION,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration TEXT,
    sessionId INT,
    song TEXT,
    status TEXT,
    ts BIGINT,
    userAgent TEXT,
    userId TEXT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INT,
    artist_id TEXT,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration TEXT,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    start_time BIGINT NOT NULL,
    user_id TEXT NOT NULL,
    level TEXT,
    artist_id TEXT,
    song_id TEXT,
    session_id TEXT,
    location TEXT,
    user_agent TEXT,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time));
""")

user_table_create = ("""
CREATE TABLE users(
    user_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT)
    distkey(user_id) sortkey(user_id);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT,
    year INT,
    duration DECIMAL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id))
    distkey(song_id) sortkey(song_id);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude DECIMAL,
    longitude DECIMAL)
    distkey(artist_id) sortkey(artist_id);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time BIGINT PRIMARY KEY,
    timestamp TIMESTAMP,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL)
    distkey(start_time) sortkey(start_time);
""")


# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT as JSON {}
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT as JSON 'auto'
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, artist_id, song_id, session_id, location, user_agent)
SELECT DISTINCT ts, userid, level, artists.artist_id, songs.song_id, sessionid, events.location, useragent
FROM public."staging_events" events
    LEFT OUTER JOIN public."artists" artists on events.artist = artists.name
    LEFT OUTER JOIN public."songs" songs on events.song = songs.title
WHERE userid IS NOT NULL
""")

# Since users can appear multiple times with different level, we will select the last version of the record as determined by the timestamp field
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
WITH CTE AS(
    SELECT DISTINCT userId, firstName, lastName, gender, level
    , ROW_NUMBER () OVER( PARTITION BY userID ORDER BY ts DESC) as RN
    FROM public."staging_events" 
	WHERE userid IS NOT NULL
)
SELECT userId, firstName, lastName, gender, level 
FROM CTE
WHERE RN = 1
""")

song_table_insert = ("""
INSERT INTO songs (song_id, artist_id, title, year, duration)
SELECT DISTINCT song_id, artist_id, title, year, duration
FROM public."staging_songs"
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
WITH CTE AS (
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, count(1) as total 
    , ROW_NUMBER () OVER( PARTITION BY artist_id ORDER BY artist_id, total DESC) as RN
    FROM public."staging_songs"
    group by artist_id, artist_name, artist_location, artist_latitude, artist_longitude
)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM CTE
WHERE RN = 1;
""")

# including time_stamp field for analyts prefering to work with timestamps (which is most of the ones I work with, including myself)
time_table_insert = ("""
INSERT INTO time (start_time, timestamp, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
    time_stamp, 
    EXTRACT(HOUR FROM time_stamp) As hour,
    EXTRACT(DAY FROM time_stamp) As day,
    EXTRACT(WEEK FROM time_stamp) As week,
    EXTRACT(MONTH FROM time_stamp) As month,
    EXTRACT(YEAR FROM time_stamp) As year,
    EXTRACT(DOW FROM time_stamp) As weekday
FROM (
SELECT 
    distinct ts,
    '1970-01-01'::date + ts/1000 * interval '1 second' as time_stamp
FROM 
    staging_events);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
