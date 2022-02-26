import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try:
        print("Connecting to the default database...")
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=user")
        print("Connected!")
    except psycopg2.Error as e:
        print(f"ERROR: could not connect to the database!")
        print(e)

    print("Setting a new session...")
    conn.set_session(autocommit=True)
    print("Session has been set!")

    try:
        print("Getting a cursor to the database...")
        cur = conn.cursor()
        print("The cursor is gotten!")
    except psycopg2.Error as e:
        print("ERROR: could not get the cursor to the database!")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        print("Creating sparkify database with UTF8 encoding...")
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        print("Created!")
    except psycopg2.Error as e:
        print("ERROR")
        print(e)

    # close connection to default database
    print("Closing connection to default database...")
    conn.close()
    print("Connection is closed!")

    # connect to sparkify database
    try:
        print("Connecting to the sparkify database...")
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=user")
        print("Connected!")
    except psycopg2.Error as e:
        print(f"ERROR: could not connect to the sparkify database!")
        print(e)

    try:
        print("Getting a cursor to the database...")
        cur = conn.cursor()
        print("The cursor is gotten!")
    except psycopg2.Error as e:
        print("ERROR")
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    print("Dropping tables...")
    for query in drop_table_queries:
        try:
            print(f"Cursor is executing the query: {query} ...")
            cur.execute(query)
            print(f"Cursor successfully executed the query!")
        except psycopg2.Error as e:
            print(f"ERROR: cursor can not execute the query!")
            print(e)
        try:
            print("Committing the query...")
            conn.commit()
            print("The query is committed!")
        except psycopg2.Error as e:
            print(f"ERROR: the query {query} can not be committed!")
            print(e)


def create_tables(cur, conn):
    print("Creating tables...")
    for query in create_table_queries:
        try:
            print(f"Cursor is executing the query: {query} ...")
            cur.execute(query)
            print(f"The query successfully executed!")
        except psycopg2.Error as e:
            print(f"ERROR: cursor can not execute the query!")
            print(e)

        try:
            print("Committing the query...")
            conn.commit()
            print("The query committed!")
        except psycopg2.Error as e:
            print(f"ERROR: the query {query} can not be committed!")
            print(e)


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# def create_database():
#     # connect to default database
#     conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=user")
#     conn.set_session(autocommit=True)
#     cur = conn.cursor()
#
#     # create sparkify database with UTF8 encoding
#     cur.execute("DROP DATABASE IF EXISTS sparkifydb")
#     cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
#
#     # close connection to default database
#     conn.close()
#
#     # connect to sparkify database
#     conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=user")
#     cur = conn.cursor()
#
#     return cur, conn
#
#
# def drop_tables(cur, conn):
#     for query in drop_table_queries:
#         cur.execute(query)
#         conn.commit()
#
#
# def create_tables(cur, conn):
#     for query in create_table_queries:
#         cur.execute(query)
#         conn.commit()
#
#
# def main():
#     cur, conn = create_database()
#
#     drop_tables(cur, conn)
#     create_tables(cur, conn)
#
#     conn.close()
#
#
# if __name__ == "__main__":
#     main()