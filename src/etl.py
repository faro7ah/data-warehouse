import config
import psycopg2
import sql_queries
import time


# A reference to the builtin function 'print()'.
builtin_print = print


def print(text):

    """
    Prints a timestamp next to the the given text.

    Args:
        text (str): The text to print.
    """

    return builtin_print('{} | {}'.format(
        time.strftime('%H:%M:%S', time.gmtime()),
        text
    ))


def load_staging_tables():

    """
    Populates the database Sparkify.
    """

    with psycopg2.connect(config.SPARKIFYDB_DSN) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:

            # Copies the events from S3 to the 'staging_events' table.
            print('Copying events into the staging table \'staging_events\'')
            cur.execute(sql_queries.staging_events_copy)

            # Copies the songs from S3 to the 'staging_songs' table.
            print('Copying songs into the staging table \'staging_songs\'')
            counter = 1
            for query in sql_queries.staging_songs_copies():
                print(' --> Batch {}'.format(counter))
                cur.execute(query)
                counter += 1

            # Inserts the users using the staging tables as source.
            print('Inserting records in the table \'users\'')
            cur.execute(sql_queries.users_table_insert)

            # Inserts the songs using the staging tables as source.
            print('Inserting records in the table \'songs\'')
            cur.execute(sql_queries.songs_table_insert)

            # Inserts the artists using the staging tables as source.
            print('Inserting records in the table \'artists\'')
            cur.execute(sql_queries.artists_table_insert)

            # Inserts the timestamps using the staging tables as source.
            print('Inserting records in the table \'time\'')
            cur.execute(sql_queries.time_table_insert)

            # Inserts the songplays using the staging tables as source.
            print('Inserting records in the table \'songplays\'')
            cur.execute(sql_queries.songplays_table_insert)


if __name__ == "__main__":
    load_staging_tables()
    print('Database Sparkify populated :-)')
