import urllib.parse
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
import sys
from pprint import pprint
from tqdm import tqdm
import pandas as pd
import re
import string
import os
from webdriver_manager.chrome import ChromeDriverManager
import uuid


class BloomsTextScraper:
    def __init__(self):
        self.driver = uc.Chrome(version_main=107)

    def get_job_posting_page(self, url):
        self.driver.get(url)

    def get_page_text(self):
        try:
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="description-content"]')
                )
            )
        except TimeoutException:
            print("Waited too long")
            return
        desc = self.driver.find_element("xpath", '//*[@id="description-content"]').text
        try:
            min_exp = self.driver.find_element(
                "xpath", '//*[@id="min_experience"]'
            ).text
        except:
            min_exp = "None required"

        last_posted_date = self.driver.find_element(
            "xpath", '//*[@id="last_posted_date"]'
        ).text
        address = self.driver.find_element("xpath", '//*[@id="address"]').text
        company = self.driver.find_element(
            "xpath", '//*[@data-cy="company-hire-info__company"]'
        ).text
        return [desc, min_exp, last_posted_date, address, company]

    def get_n_page_text(self, link_list):
        try:
            text_frame = pd.DataFrame(
                None,
                columns=[
                    "job_link",
                    "job_text",
                    "min_exp",
                    "last_posted_date",
                    "address",
                    "company",
                ],
            )
            for i, link in tqdm(enumerate(link_list)):
                self.get_job_posting_page(link)
                data = self.get_page_text()
                if data:
                    text_frame.loc[text_frame.shape[0]] = [link] + data
                else:
                    continue
                if (i + 1) % 50 == 0:
                    print(f"{os.getpid()} REACHED MILESTONE NUM: {(i+1)/50}")
                    text_frame.to_csv(
                        f"./webscraper/textscraper_chunks.nosync/{str(uuid.uuid4())}_{i+1}.csv",
                        index=False,
                    )
                    text_frame = pd.DataFrame(
                        None,
                        columns=[
                            "job_link",
                            "job_text",
                            "min_exp",
                            "last_posted_date",
                            "address",
                            "company",
                        ],
                    )
        except Exception as e:
            print(e)
        finally:
            return

    def kill_driver(self):
        self.driver.quit()


# The scrapping class used to get job posting links
class BloomsWebScraper:
    def __init__(self, positionLevel):
        self.driver = uc.Chrome(version_main=107)
        self.positionLevel = positionLevel

    def get_target_url(self, n):
        jobLevel = urllib.parse.quote(self.positionLevel)
        BASE_URL = r"https://www.mycareersfuture.gov.sg/search?"
        QUERY_URL = f"positionLevel={jobLevel}&sortBy=relevancy&page={n}"
        TARGET_URL = BASE_URL + QUERY_URL
        self.driver.get(TARGET_URL)
        return

    def process_salary_data(self, data):
        r = data.translate(str.maketrans("", "", string.punctuation))
        r = re.sub("to", " ", r).split(" ")
        return r

    def process_scrapped_url(self, url):
        industry = url.split("/")[4]
        return [industry, self.positionLevel]

    def get_page_info(self):
        try:
            WebDriverWait(self.driver, 180).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="search-results"]/div[3]')
                )
            )
        except TimeoutException:
            print("Waited too long")
            return

        # Scrapping data
        elems = self.driver.find_elements("xpath", "//a[@href]")
        scrapped_urls = [
            e.get_attribute("href") for e in elems if "job" in e.get_attribute("href")
        ]
        s_elems = self.driver.find_elements("xpath", '//*[@data-cy="salary-range"]')
        scrapped_salary = [self.process_salary_data(e.text) for e in s_elems]

        a_elems = self.driver.find_elements(
            "xpath", '//*[@data-cy="job-card__num-of-applications"]'
        )
        if len(a_elems) > 0:
            scrapped_app_count = [e.text for e in a_elems]
        else:
            scrapped_app_count = []

        other_url_info = list(map(self.process_scrapped_url, scrapped_urls))
        other_url_df = pd.DataFrame(
            data=other_url_info, columns=["industry", "position_level"]
        )
        # Creating the result frame
        result_frame = pd.DataFrame(None)
        result_frame["job_link"] = pd.Series(scrapped_urls, dtype=str)
        result_frame["num_of_applications"] = pd.Series(scrapped_app_count)
        result_frame["num_of_applications"] = (
            result_frame["num_of_applications"]
            .fillna("0")
            .apply(
                lambda x: re.findall(r"([0-9]+)", x)[0]
                if len(re.findall(r"([0-9]+)", x)) > 0
                else 0
            )
        )
        result_frame["job_salary"] = pd.Series(scrapped_salary)
        result_frame["lower_salary_range"] = result_frame["job_salary"].map(
            lambda x: min(x) if isinstance(x, list) else 0
        )
        result_frame["upper_salary_range"] = result_frame["job_salary"].map(
            lambda x: max(x) if isinstance(x, list) else 0
        )
        result_frame.pop("job_salary")
        result_frame = pd.concat([result_frame, other_url_df], axis=1)
        return result_frame

    def get_n_page_links(self, n):
        try:
            k = 0
            n_links = []
            print(f"{os.getpid()} STARTING JOB.")
            with tqdm(total=n) as pb:
                while k < n:
                    self.get_target_url(k)
                    k += 1
                    temp_urls = self.get_page_info()
                    pl = self.positionLevel.translate(
                        str.maketrans("", "", string.punctuation)
                    )
                    temp_urls.to_csv(
                        f"./webscraper/webscraper_chunks.nosync/{pl}_{k}.csv",
                        index=False,
                    )
                    print(f"SAVED {pl}_{k}.csv")
                    pb.update(1)
            print(f"{os.getpid()} JOB DONE")
            return
        except KeyboardInterrupt:
            print("Killed process.")
            return

    def kill_driver(self):
        self.driver.quit()
