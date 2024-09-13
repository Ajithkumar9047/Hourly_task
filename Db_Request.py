import pyodbc
import logging
from Config import *

server = '10.121.2.43'
database = 'TVSM_BRAND_WEBSITE'
username = 'ePageMaker'
password = 'RTRabs180'
driver = '{ODBC Driver 17 for SQL Server}'

conn = None
cursor = None

def create_connection():
    global conn, cursor  # Declare conn and cursor as global variables
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print("Failed to establish a database connection:", str(e))
        logging.error("Failed to establish a database connection: %s", str(e))

def establish_connection(query):
    if conn is None:
        create_connection()

    global cursor  # Declare cursor as a global variable
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print("Failed to execute the query:", str(e))
        logging.error("Failed to execute the query: %s", str(e))
        return None
def close_connection():
    global cursor, conn  # Declare cursor and conn as global variables
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
