import psycopg2
from psycopg2.extras import execute_values

POSTGRESQL_DB_NAME = 'mexc3'
POSTGRESQL_USER = 'postgres'
POSTGRESQL_PASSWORD = '1374fjsney831'
POSTGRESQL_HOST = 'localhost'

def sql_put(sqlite_request, data_tuple):
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        cursor.execute(sqlite_request, data_tuple)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)

def sql_get(sqlite_request, data_tuple):
    results = []
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        cursor.execute(sqlite_request, data_tuple)
        results = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)
    return results

def sql_execute_values(sql_request, data):
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        execute_values(cursor, sql_request, data)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)