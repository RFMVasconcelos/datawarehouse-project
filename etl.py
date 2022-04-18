import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Executes COPY queries that Load data from S3 into staging tables
    - Prints when completed each stage of loading
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('Completed loading staging table - ', query)


def insert_tables(cur, conn):
    """
    - Executes INSERT INTO queries that Extracts data from staging tables, Transforms, and Loads into final OLAP tables
    - Prints when completed each stage of loading
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('Completed Loading final table - ', query)


def main():
    """
    - Reads config from dwh.cfg 
    - Connects to redshift cluster db using psycopg2
    - Creates a cursor 
    - Calls in the `load_staging_tables` function to COPY data from S3 into staging tables
    - Calls in the `insert_tables` function to Transform and Load data into final tables
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()