import config

# ---------------------- #
# Table 'staging_events' #
# ---------------------- #

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
              eventId INTEGER
                      IDENTITY(0, 1)
                      PRIMARY KEY,
               artist VARCHAR
                      DISTKEY,
                 auth VARCHAR,
            firstName VARCHAR,
               gender VARCHAR(1),
        itemInSession INTEGER,
             lastName VARCHAR,
               length FLOAT,
                level VARCHAR,
             location VARCHAR,
               method VARCHAR(7),
                 page VARCHAR,
         registration BIGINT,
            sessionId INTEGER,
                 song VARCHAR
                      SORTKEY,
               status SMALLINT,
                   ts TIMESTAMP,
            userAgent VARCHAR,
               userId INTEGER
    )
    DISTSTYLE KEY;
"""

staging_events_copy = """
               COPY staging_events
               FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
      TIMEFORMAT AS 'epochmillisecs'
             REGION '{}'
               JSON '{}'
    TRUNCATECOLUMNS
       BLANKSASNULL
        EMPTYASNULL;
""".format(
    config.S3_LOG_DATA,
    config.IAM_ROLE_ARN,
    config.AWS_REGION,
    config.S3_LOG_JSON_PATH
)

# --------------------- #
# Table 'staging_songs' #
# --------------------- #

staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
                 song_id VARCHAR
                         PRIMARY KEY
                         SORTKEY,
               num_songs INTEGER,
                   title VARCHAR(1024),
             artist_name VARCHAR(1024),
         artist_latitude FLOAT,
                    year SMALLINT,
                duration FLOAT,
               artist_id VARCHAR
                         DISTKEY,
        artist_longitude FLOAT,
         artist_location VARCHAR(1024)
    )
    DISTSTYLE KEY;
"""


def staging_songs_copies():

    """
    Generate the queries to copy song data from S3 to the staging table.

    Yields:
        (str): The queries to copy song data in batches.
    """

    for char in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':

        yield """
                       COPY staging_songs
                       FROM '{}/{}'
                CREDENTIALS 'aws_iam_role={}'
                     REGION '{}'
                       JSON 'auto'
            TRUNCATECOLUMNS
               BLANKSASNULL
                EMPTYASNULL;
        """.format(
            config.S3_SONG_DATA,
            char,
            config.IAM_ROLE_ARN,
            config.AWS_REGION
        )

# ----------------- #
# Table 'songplays' #
# ----------------- #

songplays_table_drop = "DROP TABLE IF EXISTS songplays;"

songplays_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER
                    IDENTITY(0, 1)
                    PRIMARY KEY,
         start_time TIMESTAMP
                    NOT NULL
                    REFERENCES time(start_time),
            user_id INTEGER
                    NOT NULL
                    REFERENCES users(user_id),
              level VARCHAR
                    NOT NULL,
            song_id VARCHAR
                    NOT NULL
                    REFERENCES songs(song_id)
                    SORTKEY,
          artist_id VARCHAR
                    NOT NULL
                    REFERENCES artists(artist_id)
                    DISTKEY,
         session_id INTEGER
                    NOT NULL,
           location VARCHAR
                    NOT NULL,
         user_agent VARCHAR
                    NOT NULL
    )
    DISTSTYLE KEY;
"""

songplays_table_insert = """
    INSERT INTO songplays (
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent)
         SELECT staging_events.ts AS start_time,
                staging_events.userId AS user_id,
                staging_events.level,
                staging_songs.song_id,
                staging_songs.artist_id,
                staging_events.sessionId AS session_id,
                staging_events.location,
                staging_events.userAgent AS user_agent
           FROM staging_events
           JOIN staging_songs
             ON staging_events.artist = staging_songs.artist_name
            AND staging_events.song = staging_songs.title
          WHERE staging_events.page = 'NextSong';
"""

# ------------- #
# Table 'users' #
# ------------- #

users_table_drop = "DROP TABLE IF EXISTS users;"

users_table_create = """
    CREATE TABLE IF NOT EXISTS users (
           user_id INTEGER
                   PRIMARY KEY,
        first_name VARCHAR
                   NOT NULL,
         last_name VARCHAR
                   NOT NULL,
            gender VARCHAR(1)
                   NOT NULL,
             level VARCHAR
                   NOT NULL
    )
    DISTSTYLE ALL;
"""

users_table_insert = """
    INSERT INTO users (
                user_id,
                first_name,
                last_name,
                gender,
                level)
         SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
           FROM staging_events
          WHERE user_id IS NOT NULL;
"""

# ------------- #
# Table 'songs' #
# ------------- #

songs_table_drop = "DROP TABLE IF EXISTS songs;"

songs_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
          song_id VARCHAR
                  PRIMARY KEY,
            title VARCHAR(1024)
                  NOT NULL,
        artist_id VARCHAR
                  NOT NULL
                  REFERENCES artists(artist_id)
                  DISTKEY
                  SORTKEY,
             year SMALLINT,
         duration FLOAT
    )
    DISTSTYLE KEY;
"""

songs_table_insert = """
    INSERT INTO songs (
                song_id,
                title,
                artist_id,
                year,
                duration)
         SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
           FROM staging_songs
          WHERE song_id IS NOT NULL;
"""

# --------------- #
# Table 'artists' #
# --------------- #

artists_table_drop = "DROP TABLE IF EXISTS artists;"

artists_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR
                  PRIMARY KEY,
             name VARCHAR(1024)
                  NOT NULL,
         location VARCHAR(1024),
         latitude FLOAT,
        longitude FLOAT
    )
    DISTSTYLE ALL;
"""

artists_table_insert = """
    INSERT INTO artists (
                artist_id,
                name,
                location,
                latitude,
                longitude)
         SELECT DISTINCT artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
           FROM staging_songs
          WHERE artist_id IS NOT NULL;
"""

# ------------- #
# Table 'time' #
# ------------- #

time_table_drop = "DROP TABLE IF EXISTS time;"

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP
                   PRIMARY KEY,
              hour SMALLINT
                   NOT NULL,
               day SMALLINT
                   NOT NULL,
              week SMALLINT
                   NOT NULL,
             month SMALLINT
                   NOT NULL,
              year SMALLINT
                   NOT NULL,
           weekday SMALLINT
                   NOT NULL
    )
    DISTSTYLE ALL;
"""

time_table_insert = """
    INSERT INTO time(
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday)
         SELECT DISTINCT ts AS start_time,
                DATE_PART(HOUR, ts) AS hour,
                DATE_PART(DAY, ts) AS day,
                DATE_PART(WEEK, ts) AS week,
                DATE_PART(MONTH, ts) AS month,
                DATE_PART(YEAR, ts) AS year,
                DATE_PART(WEEKDAY, ts) AS weekday
           FROM staging_events
          WHERE ts IS NOT NULL;;
"""
