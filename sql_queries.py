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

# CREATE TABLES

## Staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    event_id       int IDENTITY(0,1),
    artist_name    varchar,
    auth           varchar,
    firstName      varchar,
    gender         varchar,
    iteInSession   int,
    lastName       varchar,
    length         float, 
    level          varchar,
    location       varchar,
    method         varchar,
    page           varchar,
    registration   varchar,
    session_id     int,
    song_title     varchar,
    status         int,
    ts             varchar,
    user_agent     varchar,
    user_id        varchar,
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id          varchar,
    num_songs        int,
    artist_id        varchar,
    artist_latitude  varchar,
    artist_longitude varchar,
    artist_location  varchar,
    artist_name      varchar,
    title            varchar,
    duration         float,
    year             int,
    PRIMARY KEY(song_id)
)
""")

## OLAP tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id varchar, 
    start_time TIMESTAMP NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id varchar, 
    location varchar, 
    user_agent varchar,
    PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar,
    PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar, 
    title varchar NOT NULL, 
    artist_id varchar, 
    year int, 
    duration float NOT NULL,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar,
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float,
    PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time time, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar,
    PRIMARY KEY(start_time)
)
""")

# STEP 1: LOAD TO STAGING TABLES

staging_events_copy = (
    """
    copy         staging_events 
    from         {}
    credentials  'aws_iam_role={}'
    region       'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    json         {};
    """).format(config['S3']['LOG_DATA'], 
                config['IAM']['IAM_ARN'], 
                config['S3']['LOG_JSONPATH'])

staging_songs_copy = (
    """
    copy         staging_songs 
    from         {}
    credentials  'aws_iam_role={}'
    region       'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    json         'auto'
    """).format(config['S3']['SONG_DATA'], 
                config['IAM']['IAM_ARN'])

# STEP 2: INSERT INTO FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users 
(
user_id, first_name, last_name, gender, level
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs 
(
song_id, title, artist_id, year, duration
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists 
(
artist_id, name, location, latitude, longitude
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time 
(
start_time, hour, day, week, month, year, weekday
)
VALUES (%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
