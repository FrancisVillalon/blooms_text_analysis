import os

import psycopg
import tomllib
from tqdm import tqdm
import pandas as pd

# Initial setup of database tables and other entries

word_path = "./database/other_data/words"

# database config
with open("./config/db-config.toml", "rb") as f:
    data = tomllib.load(f)
    db_user = data["database"]["db_user"]
    db_secret = data["database"]["db_pass"]
    db_name = data["database"]["db_name"]

mapLevel = {
    "Fresh/entry level": 0,
    "Non-executive": 1,
    "Junior Executive": 2,
    "Manager": 2,
    "Executive": 3,
    "Professional": 3,
    "Middle Management": 3,
    "Senior Management": 4,
    "Senior Executive": 4,
}

# create connection and perform setup
with psycopg.connect(dbname=db_name, user=db_user, password=db_secret) as conn:
    with conn.cursor() as cur:
        # Setup blooms taxonomy terms
        print("=> Setting up blooms taxonomy terms")
        for fn in tqdm(os.listdir(word_path)):
            word_category = fn.split(".")[0]
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {word_category}_keywords (
                    keywords text unique primary key
                ) 
                """
            )
            with open(os.path.join(word_path, fn), "r") as f:
                for word in f:
                    try:
                        query = f"INSERT INTO {word_category}_keywords (keywords) VALUES (%s)"

                        params = (word.strip(),)
                        cur.execute(query, params)
                    except Exception as e:
                        conn.rollback()
                        continue
                    conn.commit()
        # Setup level to job position mapping
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS job_levels (
                job_title text unique primary key,
                job_level int

            )
            """
        )
        print("=> Setting up position level table")
        for level in tqdm(mapLevel.keys()):
            try:
                cur.execute(
                    f"INSERT INTO job_levels VALUES (%s,%s)", (level, mapLevel[level])
                )
            except Exception as e:
                conn.rollback()
                continue
            conn.commit()

        # Setup job text table
        print("=> Setting up job text table")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS job_postings (
                job_link text unique primary key,
                job_text text,
                last_posted_date date,
                address text,
                company text,
                num_of_applications int,
                lower_salary_range int,
                upper_salary_range int,
                industry text,
                position_level text,
                min_exp_years int 
            )
            """
        )
        DATA = pd.read_csv("./database/other_data/TEXTDATA.csv", index_col=0)
        DATA = DATA.reset_index()
        for i, k in tqdm(DATA.iterrows(), total=DATA.shape[0]):
            try:
                query = f"INSERT INTO job_postings VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                params = tuple(k.values)
                cur.execute(query, params)
                print(f"INSERTED INTO job_postings: {params[0]}")
            except Exception as e:
                conn.rollback()
                continue
            # commit transactions
            conn.commit()
