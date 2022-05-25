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

# Knowledge thread: knowledge.udacity.com/questions/505535
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    method varchar,
    page varchar,
    registration float,
    session_id integer,
    song varchar,
    status integer,
    ts bigint,
    user_agent varchar,
    user_id varchar
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id varchar,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    duration float,
    num_songs integer,
    title varchar,
    year integer
);   
""")


songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id integer,
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id varchar PRIMARY KEY,
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar PRIMARY KEY, 
    title varchar,
    artist_id varchar NOT NULL, 
    year integer, 
    duration float
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar PRIMARY KEY,
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer,
    weekday integer
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

# Knowledge thread: knowledge.udacity.com/questions/657324
# Knowledge thread: knowledge.udacity.com/questions/792847
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time,
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page='NextSong'
    AND se.song = ss.title 
    AND se.artist = ss.artist_name 
    AND se.length = ss.duration 
""")

# Knowledge thread: knowledge.udacity.com/questions/444676
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name,gender, level)
    SELECT 
        DISTINCT se.user_id,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_events se
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT 
        DISTINCT ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
""")

# Knowledge thread: knowledge.udacity.com/questions/631868
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
