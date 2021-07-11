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

staging_events_table_create = ("""
    CREATE TABLE staging_events (
          artist              varchar(200)    not null
        , auth                varchar(30)     not null
        , first_name          varchar(50)     not null
        , gender              char(1)         not null
        , item_in_session     integer         not null
        , last_name           varchar(100)    not null
        , length              decimal         not null
        , level               varchar(10)     not null
        , location            varchar(200)    not null
        , method              varchar(10)     not null
        , page                varchar(20)     not null
        , registration        decimal         not null
        , session_id          integer         not null
        , song                varchar(200)    not null
        , status              integer         not null
        , user_agent          varchar(200)    not null
        , user_id             integer         not null
    )
    diststyle auto;
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
          num_songs           integer         not null
        , artist_id           varchar(30)     not null
        , artist_latitude     decimal
        , artist_longitude    decimal
        , artist_location     varchar(200)
        , artist_name         varchar(100)    not null
        , song_id             varchar(30)     not null
        , title               varchar(200)    not null
        , duration            decimal
        , year                smallint
    )
    diststyle auto;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY
    , start_time bigint NOT NULL
    , user_id int NOT NULL
    , level varchar NOT NULL
    , song_id varchar
    , artist_id varchar
    , session_id int NOT NULL
    , location varchar NOT NULL
    , user_agent varchar NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY
    , first_name varchar NOT NULL
    , last_name varchar NOT NULL
    , gender varchar(1) NOT NULL
    , level varchar(10) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY
    , title varchar NOT NULL
    , artist_id varchar NOT NULL
    , year smallint NOT NULL
    , duration float NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY
    , name varchar NOT NULL
    , location varchar
    , latitude float
    , longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY
    , hour smallint NOT NULL
    , day smallint NOT NULL
    , week smallint NOT NULL
    , month smallint NOT NULL
    , year smallint NOT NULL
    , weekday smallint NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
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

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
