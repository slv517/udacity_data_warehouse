import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function copies the data in the S3 buckets into the staging tables in the Redshift cluster."""
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This function gets the data in the staging tables and moves it into the fact/dimension tables."""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function connects to the Redshift database first. It then moves the data from the S3 buckets into the staging tables and then finally into the fact/dimension tables created. It then closes the connection to the database."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()