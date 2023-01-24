from webscraper.ws import *
from multiprocessing import Pool
import pandas as pd
import traceback
import pandas.io.sql as sqlio
import sys
import os
import uuid


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


def init_elementscrape(
    link_list,
) -> "Added to obtain data in page that was not taken in initial run. Needs refactor.":
    try:
        print(f"STARTING ELEMENT SCRAPE FOR {len(link_list)} PAGES.")
        scraper = BloomsTextScraper()
        ele_frame = pd.DataFrame(
            None,
            columns=[
                "job_link",
                "element_text",
            ],
        )
        for i, link in tqdm(enumerate(link_list)):
            ele_text = scraper.get_element('//*[@id="job_title"]', link)
            if ele_text:
                ele_frame.loc[ele_frame.shape[0]] = [link, ele_text]
            if (i + 1) % 50 == 0:
                print(f"{os.getpid()} REACHED MILESTONE NUM: {(i+1)/50}")
                ele_frame.to_parquet(
                    f"./webscraper/miscscraper_chunks.nosync/{str(uuid.uuid4())}_{i+1}.parquet"
                )
                ele_frame = pd.DataFrame(
                    None,
                    columns=[
                        "job_link",
                        "element_text",
                    ],
                )
    except Exception as e:
        print(repr(e))

    finally:
        scraper.kill_driver()


def get_and_consolidate_element(
    link_list, n
) -> "Process list of links in n sized chunks":
    pool_inputs = [link_list[i : i + n] for i in range(0, len(link_list), n)]
    with Pool(processes=5) as pool:
        pool.map(init_elementscrape, pool_inputs)
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
