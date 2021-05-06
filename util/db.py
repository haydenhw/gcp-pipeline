import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        database="mig",
        user="postgres",
        password=os.getenv("POSTGRES_PW"),
        host="35.196.215.29",
        port="5432"
    )
