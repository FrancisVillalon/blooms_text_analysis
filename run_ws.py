from webscraper.ws import *
from multiprocessing import Pool
import pandas as pd
import traceback
from database.db_conn import create_conn
import pandas.io.sql as sqlio
import sys
import os


def init_webscrape(n, positionLevel):
    print(f"STARTING JOB FOR {positionLevel}")
    try:
        scraper = BloomsWebScraper(positionLevel)
    except Exception as e:
        print(e)
    finally:
        scraper.kill_driver()
        return


def init_textscrape(link_list):
    print(f"STARTING TEXT SCRAPE FOR {len(link_list)} PAGES.")
    try:
        scraper = BloomsTextScraper()
        scraper.get_n_page_text(link_list)
    except Exception as e:
        print(e)
    finally:
        scraper.kill_driver()
        return


def get_and_consolidate_nlinks(
    n, job_levels
) -> "Iterate over job_levels list and grab links from n pages":
    pool_inputs = [(n, e) for e in job_levels]
    # To get links from pages
    with Pool(processes=5) as pool:
        mframe = pd.DataFrame(None)
        pool.starmap(init_webscrape, pool_inputs)
    return


def get_and_consolidate_text(link_list, n) -> "Process list of links in n sized chunks":
    pool_inputs = [link_list[i : i + n] for i in range(0, len(link_list), n)]
    with Pool(processes=5) as pool:
        pool.map(init_textscrape, pool_inputs)
    return
