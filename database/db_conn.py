import tomllib
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.engine import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import close_all_sessions
import urllib.parse


with open("./config/db-config.toml", "rb") as f:
    data = tomllib.load(f)
    db_user = data["database"]["db_user"]
    db_secret = urllib.parse.quote_plus(data["database"]["db_pass"])
    db_name = data["database"]["db_name"]


class DB_CONN:
    def __init__(self):
        # Define connection
        self.DB_STRING = f"postgresql://{db_user}:{db_secret}@127.0.0.1:5432/{db_name}"
        self.DB_ENGINE = create_engine(self.DB_STRING)

        # Define tables
        Base = automap_base()
        Base.prepare(autoload_with=self.DB_ENGINE, reflect=True)

        self.job_levels = Base.classes.job_levels
        self.job_postings = Base.classes.job_postings

        self.analyze_keywords = Base.classes.analyze_keywords
        self.apply_keywords = Base.classes.apply_keywords
        self.create_keywords = Base.classes.create_keywords
        self.evaluate_keywords = Base.classes.evaluate_keywords
        self.knowledge_keywords = Base.classes.knowledge_keywords
        self.understand_keywords = Base.classes.understand_keywords

    def create_session(self):
        return Session(self.DB_ENGINE)

    def commit_and_kill_session(self, s):
        s.commit()
        s.close()
        return

    def kill_conn(self, conn):
        conn.close()
        return

    def kill_all_sessions(self):
        close_all_sessions()
        return
