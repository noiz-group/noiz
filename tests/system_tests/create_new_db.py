import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

dbname = os.getenv("POSTGRES_DB")

con = psycopg2.connect(host="localhost", user="noiztest", password="noiztest")
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
with con.cursor() as cur:
    cur.execute(f"CREATE DATABASE {dbname}")
con.close()

with psycopg2.connect(host="localhost", user="noiztest", password="noiztest", dbname=dbname) as conn:
    with conn.cursor() as curr:
        curr.execute("CREATE EXTENSION hstore;")
