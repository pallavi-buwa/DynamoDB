import pyodbc
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def get_connection():
    conn_str = (
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def get_sql_server_schema(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = [row.TABLE_NAME for row in cursor.fetchall()]

    schema = {}
    for table in tables:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ?
        """, table)
        columns = [{"column": row.COLUMN_NAME, "type": row.DATA_TYPE} for row in cursor.fetchall()]
        schema[table] = columns

    return schema

def get_mongo_connection():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB_NAME")]
    return db

def get_mongo_schema(db, sample_size=5):
    schema = {}
    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        sample_docs = collection.find().limit(sample_size)
        fields = set()
        for doc in sample_docs:
            fields.update(doc.keys())
        schema[collection_name] = list(fields)

    return schema
