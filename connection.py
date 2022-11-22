import os
import json
import psycopg2
from sqlalchemy import create_engine

def config(connection_db):
    path = os.getcwd() # getting absolute path/directory to this file
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[connection_db]
    return conf

def psql_conn(conf, conn_name):
    try:
        conn = psycopg2.connect(
            host = conf['host'],
            database = conf['db'],
            user = conf['user'],
            password = conf['password'],
            port=conf['port']
        )
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['db']}") #engine for connecting
        print(f"[INFO] Success: Connected to PostgreSQL-{conn_name}")
        return conn, engine
    except Exception as e:
        print(f"[INFO] Can't connect to PostgreSQL-{conn_name}")
        print(str(e))

