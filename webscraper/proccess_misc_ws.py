import pandas as pd
import os
import tqdm

# Added to process misc data.
mframe = pd.DataFrame(None)
for i, chunk in enumerate(os.listdir("./webscraper/miscscraper_chunks.nosync/")):
    x = pd.read_parquet(f"./webscraper/miscscraper_chunks.nosync/{chunk}")
    mframe = pd.concat([mframe, x], axis=0)
mframe = mframe.dropna(subset="job_link")
mframe = mframe.drop_duplicates(subset="job_link")
mframe = mframe.reset_index(drop=True)
TEXTDATA = pd.read_parquet("./database/other_data/TEXTDATA.parquet")
joined_frame = TEXTDATA.merge(mframe, how="left", on="job_link")
joined_frame["job_title"] = joined_frame["element_text"]
print(joined_frame.loc[joined_frame["job_title"].isnull()])
