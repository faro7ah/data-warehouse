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

            # Create the 'staging_events' table.
            print('Creating the table \'staging_events\'')
            cur.execute(sql_queries.staging_events_table_drop)
            cur.execute(sql_queries.staging_events_table_create)

            # Create the 'staging_songs' table.
            print('Creating the table \'staging_songs\'')
            cur.execute(sql_queries.staging_songs_table_drop)
            cur.execute(sql_queries.staging_songs_table_create)

            # Create the 'songplays' table.
            print('Creating the table \'songplays\'')
            cur.execute(sql_queries.songplays_table_drop)
            cur.execute(sql_queries.songplays_table_create)

            # Create the 'users' table.
            print('Creating the table \'users\'')
            cur.execute(sql_queries.users_table_drop)
            cur.execute(sql_queries.users_table_create)

            # Create the 'songs' table.
            print('Creating the table \'songs\'')
            cur.execute(sql_queries.songs_table_drop)
            cur.execute(sql_queries.songs_table_create)

            # Create the 'artists' table.
            print('Creating the table \'artists\'')
            cur.execute(sql_queries.artists_table_drop)
            cur.execute(sql_queries.artists_table_create)

            # Create the 'time' table.
            print('Creating the table \'time\'')
            cur.execute(sql_queries.time_table_drop)
            cur.execute(sql_queries.time_table_create)


if __name__ == '__main__':
    init_database()
    print('Database Sparkify initialized :-)')
