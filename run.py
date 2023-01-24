from database.db_conn import DB_CONN
from webscraper.ws_methods import *
import pandas as pd


# db = DB_CONN()
# s = db.create_session()

# print(pd.read_sql_table("job_levels", con=s.connection()))
# q = s.query(db.job_levels).filter_by(job_title="test").first()
# s.delete(q)
# db.commit_and_kill_session(s)
# print(pd.read_sql_table("evaluate_keywords", con=s.connection()))
# db.kill_all_sessions()
if __name__ == "__main__":
    DATA_PATH = "./database/other_data/TEXTDATA.parquet"
    DATA = pd.read_parquet(DATA_PATH)
    LINK_LIST = list(DATA["job_link"].values)
    get_and_consolidate_element(LINK_LIST, 100)
