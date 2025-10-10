import mysql.connector
import os

def is_docker():
    """Detect if running inside Docker."""
    return os.path.exists('/.dockerenv')

def get_db(database=None):
    host = 'mysql' if is_docker() else '127.0.0.1'
    port = 3306 if is_docker() else 3307
    return mysql.connector.connect(
        host=host,
        user='airflow',
        password='airflow',
        database=database or 'grammy_db',
        port=port
    )

def get_db_connection():
    """Return MySQL connection depending on environment (Docker vs Host)."""
    host = 'mysql' if is_docker() else '127.0.0.1'
    port = 3306 if is_docker() else 3307  # 3307 es el puerto mapeado al host
    conn = mysql.connector.connect(
        host=host,
        user='airflow',
        password='airflow',
        database='grammy_db',
        port=port
    )
    return conn

def get_db_connection_string():
    return "mysql+mysqlconnector://airflow:airflow@mysql:3306/grammy_db"