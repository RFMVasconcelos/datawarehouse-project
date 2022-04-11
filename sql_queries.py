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

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
event_id serial PRIMARY KEY,
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession int,
lastName varchar,
length float,
level varchar,
location varchar, 
method varchar,
page varchar,
registration float,
sessionId int,
song varchar, 
status int,
ts timestamp,
userAgent varchar, 
userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
songs_event_id serial PRIMARY KEY,
num_songs smallint,
artist_id varchar,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year smallint,
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY, 
start_time TIMESTAMP NOT NULL, 
user_id serial NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id varchar, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar, 
year int, 
duration float NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY,
name varchar NOT NULL, 
location varchar, 
latitude float, 
longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time time PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role=ARN'

    region 'us-west-2';
""").format()

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role=ARN'
    region 'us-west-2';
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
