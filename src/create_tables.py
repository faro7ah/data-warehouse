import config
import psycopg2
import sql_queries


def init_database():

    """
    Initializes the database Sparkify.
    """

    with psycopg2.connect(config.SPARKIFYDB_DSN) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:

            # Drops the tables.
            print('Dropping tables')
            print(' --> songplays')
            cur.execute(sql_queries.songplays_table_drop)
            print(' --> songs')
            cur.execute(sql_queries.songs_table_drop)
            print(' --> artists')
            cur.execute(sql_queries.artists_table_drop)
            print(' --> users')
            cur.execute(sql_queries.users_table_drop)
            print(' --> time')
            cur.execute(sql_queries.time_table_drop)
            print(' --> staging_songs')
            cur.execute(sql_queries.staging_songs_table_drop)
            print(' --> staging_events')
            cur.execute(sql_queries.staging_events_table_drop)

            # Creates the tables.
            print('Creating tables')
            print(' --> staging_events')
            cur.execute(sql_queries.staging_events_table_create)
            print(' --> staging_songs')
            cur.execute(sql_queries.staging_songs_table_create)
            print(' --> time')
            cur.execute(sql_queries.time_table_create)
            print(' --> users')
            cur.execute(sql_queries.users_table_create)
            print(' --> artists')
            cur.execute(sql_queries.artists_table_create)
            print(' --> songs')
            cur.execute(sql_queries.songs_table_create)
            print(' --> songplays')
            cur.execute(sql_queries.songplays_table_create)


if __name__ == '__main__':
    init_database()
    print('Database Sparkify initialized :-)')
