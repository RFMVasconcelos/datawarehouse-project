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
    artist_latitude  float,
    artist_longitude float,
    artist_location  varchar,
    artist_name      varchar,
    title            varchar,
    duration         float,
    year             int,
    PRIMARY KEY(song_id)
)
""")

## OLAP tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id      varchar, 
    first_name   varchar, 
    last_name    varchar, 
    gender       varchar, 
    level        varchar,
    PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   varchar, 
    title     varchar NOT NULL, 
    artist_id varchar, 
    year      int, 
    duration  float NOT NULL,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id  varchar,
    name       varchar NOT NULL, 
    location   varchar, 
    latitude   float, 
    longitude  float,
    PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time time, 
    hour       int, 
    day        int, 
    week       int, 
    month      int, 
    year       int, 
    weekday    varchar,
    PRIMARY KEY(start_time)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   int IDENTITY(0,1), 
    start_time    TIMESTAMP NOT NULL  REFERENCES time(start_time), 
    user_id       varchar   NOT NULL  REFERENCES users(user_id), 
    level         varchar, 
    song_id       varchar             REFERENCES songs(song_id), 
    artist_id     varchar             REFERENCES artists(artist_id), 
    session_id    varchar, 
    location      varchar, 
    user_agent    varchar,
    PRIMARY KEY(songplay_id)
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

user_table_insert = ("""
INSERT INTO 
    users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id, 
    firstName, 
    lastName, 
    gender, 
    level
FROM 
    staging_events
WHERE 
    page = 'NextSong'
AND user_id NOT IN (SELECT DISTINCT user_id FROM users) 
""")

song_table_insert = ("""
INSERT INTO 
    songs (song_id, title, artist_id, year, duration)
(
SELECT DISTINCT
    song_id, 
    title, 
    artist_id,
    year, 
    duration
FROM 
    staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
)
""")

artist_table_insert = ("""
INSERT INTO 
    artists (artist_id, name, location, latitude, longitude)
(
SELECT DISTINCT
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM 
    staging_songs 
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
)
""")

time_table_insert = ("""
INSERT INTO 
    time (start_time, hour, day, week, month, year, weekday)
SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
FROM (
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time 
    FROM 
        staging_events s     
    )
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

songplay_table_insert = ("""
INSERT INTO 
    songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
    e.user_id, 
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM 
    staging_events e, 
    staging_songs  s
WHERE e.page = 'NextSong'
AND   e.song_title = s.title
AND   user_id NOT IN (SELECT DISTINCT songplays.user_id 
                            FROM  songplays
                            WHERE songplays.user_id = user_id
                            AND   songplays.start_time = start_time 
                            AND   songplays.session_id = session_id )
""")

# QUERY LISTS

## Create
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

## Drop
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

## Staging Load
copy_table_queries = [staging_events_copy, staging_songs_copy]

## Final Load
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
