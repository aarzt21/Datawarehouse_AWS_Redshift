import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES: 1. Facts, 4. Dimensions and 2. Staging
#A staging table is just a temporary table containing the modified and/or cleaned business data.
#Staging table is needed so as to cleanse the data before pumping into data warehouse.


#interim table for events data
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          varchar,
        auth            varchar,
        firstName       varchar,
        gender          varchar,
        itemInSession   varchar,
        lastName        varchar,
        length          float, 
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    bigint, 
        sessionID       bigint, 
        song            varchar,
        status          int, 
        ts              bigint, 
        userAgent       varchar,
        userId          int
    );
""")


#interim table for song data
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           varchar,
        artist_name         varchar,
        artist_latitude     float,
        artist_location     varchar,
        artist_longitude    float,
        duration            float,
        num_songs           int,
        song_id             varchar,
        title               varchar,
        year                int
    );
""")


## Star Schema

#fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id         bigint, 
        start_time          timestamp,
        user_id             int, 
        level               varchar,
        song_id             varchar,
        artist_id           varchar,
        session_id          bigint, 
        location            varchar, 
        user_agent          varchar
    );
""")

#dim users
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id             varchar, 
        first_name          varchar, 
        last_name           varchar, 
        gender              varchar, 
        level               varchar
    );
""")

#dim songs
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id             varchar, 
        title               varchar, 
        artist_id           varchar, 
        year                int, 
        duration            float
    );
""")

#dim artist
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id           varchar, 
        name                varchar, 
        location            varchar, 
        latitude            varchar, 
        longitude           varchar
    );
""")


#dim time
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time          timestamp, 
        hour                int, 
        day                 int, 
        week                int, 
        month               int, 
        year                int,    
        weekday             int
    );
""")


#Step 1: Load the data from the raw source (S3 Bucket) into the staging (interim) tables
#we use the COPY command because INSERT row by row will be super slow
staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON '{}';
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
    """).format(config['S3']['song_data'], config['IAM_ROLE']['arn'])



# Step 2: transfer data from staging tables into final tables

#Fill facts table with data from staging tables (n data points)
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
            TIMESTAMP 'epoch' + se.ts * interval '1 second' AS start_time,
            se.userId        AS user_id,
            se.level         AS level,
            ss.song_id       AS song_id,
            ss.artist_id     AS artist_id,
            se.sessionId     AS session_id,
            se.location      AS location,
            se.userAgent     AS user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
    AND se.song = ss.title
    AND se.artist = ss.artist_name;
""")

#IMPORTANT: We use distinct here because the same user can appear multiple times staging_events
#(k < n data points)
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userID, firstName, lastName, gender, level 
    FROM staging_events
    WHERE userID IS NOT NULL; 
""")

#(p < n data points)
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs 
    WHERE artist_id IS NOT NULL; 
""")

#(s < n data points)
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL; 
""")

#(t < n data points)
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
                    TIMESTAMP 'epoch' + ts * interval '1 second' AS start_time,
                    EXTRACT (hour FROM start_time),
                    EXTRACT (day FROM start_time),
                    EXTRACT (week FROM start_time),
                    EXTRACT (month FROM start_time),
                    EXTRACT (year FROM start_time),
                    EXTRACT (weekday FROM start_time)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
