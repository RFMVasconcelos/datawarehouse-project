import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Executes DROP table queries 
    - Prints when completed each table drop
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Dropped table - ' + query )


def create_tables(cur, conn):
    """
    - Executes CREATE table queries 
    - Prints when completed each table created
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Created table - ' + query )


def main():
    """
    - Reads config from dwh.cfg 
    - Connects to redshift cluster db using psycopg2
    - Creates a cursor 
    - Calls in the `drop_tables` function to DROP existing tables
    - Calls in the `create_tables` function to CREATE new tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()