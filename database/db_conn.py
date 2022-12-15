import tomllib
import psycopg


with open("./config/db-config.toml", "rb") as f:
    data = tomllib.load(f)
    db_user = data["database"]["db_user"]
    db_secret = data["database"]["db_pass"]
    db_name = data["database"]["db_name"]


def create_conn():
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_secret)
    cur = conn.cursor()
    return conn, cur
