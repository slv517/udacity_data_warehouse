import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function drops the specified tables using the sql DROP queries in the sql_queries.py file"""
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function creates the specified tables using the sql CREATE TABLE queries in the sql_queries.py file."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function will firstly connect to the Redshift Database and then drop and create the required tables. Finally, it will close the connection to the Database."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()