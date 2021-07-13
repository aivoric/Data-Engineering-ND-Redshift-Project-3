# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
          artist                VARCHAR(200)
        , auth                  VARCHAR(30)
        , first_name            VARCHAR(50)
        , gender                CHAR(1)
        , item_in_session       INTEGER
        , last_name             VARCHAR(100)
        , length                FLOAT
        , level                 VARCHAR(10)
        , location              VARCHAR(200)
        , method                VARCHAR(4)
        , page                  VARCHAR(20)
        , registration          FLOAT
        , session_id            INTEGER         
        , song                  VARCHAR(200)
        , status                INTEGER
        , ts                    BIGINT
        , user_agent            VARCHAR(200)
        , user_id               INTEGER
    )
    diststyle auto;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
          num_songs             INTEGER
        , artist_id             VARCHAR(30)
        , artist_latitude       FLOAT
        , artist_longitude      FLOAT
        , artist_location       VARCHAR(200)
        , artist_name           VARCHAR(300)
        , song_id               VARCHAR(30)
        , title                 VARCHAR(300)
        , duration              FLOAT
        , year                  SMALLINT
    )
    diststyle auto;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id             BIGINT IDENTITY(0, 1)   NOT NULL  
        , start_time            BIGINT                  NOT NULL
        , user_id               INTEGER                 
        , level                 VARCHAR(10)             NOT NULL
        , song_id               VARCHAR(30)             
        , artist_id             VARCHAR(30)             
        , session_id            INTEGER                 NOT NULL
        , location              VARCHAR(200)            NOT NULL
        , user_agent            VARCHAR(200)            NOT NULL
        , primary key(songplay_id)
    )
    diststyle auto;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id                 INTEGER                 NOT NULL
        , first_name            VARCHAR(50)             NOT NULL
        , last_name             VARCHAR(100)            NOT NULL
        , gender                VARCHAR(1)              NOT NULL
        , level                 VARCHAR(10)             NOT NULL
        , primary key(user_id)
    )
    diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id                 VARCHAR(30)             NOT NULL
        , title                 VARCHAR(300)            NOT NULL
        , artist_id             VARCHAR(30)             NOT NULL
        , year                  SMALLINT                NOT NULL
        , duration              FLOAT                   NOT NULL
        , primary key(song_id)
    )
    diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id               VARCHAR(30)             NOT NULL
        , name                  VARCHAR(300)            NOT NULL
        , location              VARCHAR(200)
        , latitude              FLOAT
        , longitude             FLOAT
        , primary key(artist_id)
    )
    diststyle auto;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time              BIGINT                  NOT NULL
        , hour                  SMALLINT                NOT NULL
        , day                   SMALLINT                NOT NULL
        , week                  SMALLINT                NOT NULL
        , month                 SMALLINT                NOT NULL
        , year                  SMALLINT                NOT NULL
        , weekday               SMALLINT                NOT NULL
        , primary key(start_time)
    )
    diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
""")

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto'
""")


# STORED PROCEDURES for ANALYTICAL TABLE UPSERTS
songplays_upsert = ("""
    CREATE OR REPLACE PROCEDURE songplays_upsert()
    AS $$
    BEGIN
        DROP TABLE IF EXISTS songplays_stage;
    
        CREATE TEMP TABLE songplays_stage (LIKE songplays  INCLUDING DEFAULTS); 

        INSERT INTO songplays_stage(
                start_time
                , user_id
                , level
                , song_id
                , artist_id
                , session_id
                , location
                , user_agent) 
                (SELECT
                    se.ts
                    , se.user_id
                    , se.level
                    , ss.song_id
                    , ss.artist_id
                    , se.session_id
                    , se.location
                    , se.user_agent
                FROM staging_events se
                LEFT JOIN staging_songs ss ON ss.title = se.song
                WHERE page = 'NextSong'
                );

        DELETE FROM songplays  
        USING songplays_stage 
        WHERE songplays.session_id = songplays_stage.session_id
        AND songplays.start_time = songplays_stage.start_time;
        
        INSERT INTO songplays(
                start_time
                , user_id
                , level
                , song_id
                , artist_id
                , session_id
                , location
                , user_agent) 
                (SELECT
                    ss.start_time
                    , ss.user_id
                    , ss.level
                    , ss.song_id
                    , ss.artist_id
                    , ss.session_id
                    , ss.location
                    , ss.user_agent
                FROM songplays_stage ss
                );
        
        DROP TABLE songplays_stage;
    END;
    $$ LANGUAGE plpgsql;
""")


# ANALYTICAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time
        , user_id
        , level
        , song_id
        , artist_id
        , session_id
        , location
        , user_agent) 
        (SELECT
            se.ts
            , se.user_id
            , se.level
            , ss.song_id
            , ss.artist_id
            , se.session_id
            , se.location
            , se.user_agent
        FROM staging_events se
        LEFT JOIN staging_songs ss ON ss.title = se.song
        WHERE page = 'NextSong'
        )
""")

user_table_insert = ("""
    INSERT INTO users(
        SELECT
            a.user_id
            , a.first_name
            , a.last_name
            , a.gender
            , a.level
        FROM staging_events a
        INNER JOIN (
            SELECT user_id, MAX(ts) as ts
            FROM staging_events
            GROUP BY user_id
        ) b ON a.user_id = b.user_id AND a.ts = b.ts
        )
""")

song_table_insert = ("""
    INSERT INTO songs(
        SELECT
            DISTINCT ss.song_id
            , ss.title
            , ss.artist_id
            , ss.year
            , ss.duration
        FROM staging_songs ss
        WHERE song_id IS NOT NULL
        )
""")

artist_table_insert = ("""
    INSERT INTO artists(
        SELECT
            DISTINCT ss.artist_id
            , artist_name AS name
            , artist_location AS location
            , artist_latitude AS latitude
            , artist_longitude AS longtitude
        FROM staging_songs ss
        WHERE artist_id IS NOT NULL
        )
""")

time_table_insert = ("""
    INSERT INTO time(
        SELECT
            DISTINCT ts AS start_time
            , EXTRACT(hour FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour
            , EXTRACT(day FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day
            , EXTRACT(week FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week
            , EXTRACT(month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month
            , EXTRACT(year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year
            , EXTRACT(weekday FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
        FROM staging_events se
        )
""")

read_total_users = ("""
    SELECT
        COUNT(*) as total_users
    FROM users
""")

read_total_songs = ("""
    SELECT
        COUNT(*) as total_songs
    FROM songs
""")

read_popular_songs = ("""
    SELECT
        song_id,
        COUNT(*) as total_songplays
    FROM songplays
    WHERE song_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
""")

read_top_active_users = ("""
    SELECT
        user_id,
        COUNT(*) as total_songplays
    FROM songplays
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
""")

# QUERY LISTS
# -----------------------------------

# Staging queries list:
create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]

# Analytical queries list:
create_analytical_table_queries = [songplay_table_create, user_table_create, 
                                   song_table_create, artist_table_create, time_table_create]
drop_analytical_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, 
                                 artist_table_drop, time_table_drop]
insert_analytical_table_queries = [user_table_insert, song_table_insert, 
                                   artist_table_insert, time_table_insert]
read_analytical_tables_queries = [read_popular_songs, read_top_active_users]