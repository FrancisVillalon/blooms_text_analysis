import pandas as pd
import os
from datetime import datetime, date

pd.set_option("display.max_columns", None)
TEXT_OUT_PATH = "./database/other_data/process_output/consolidated_text/"
LINK_OUT_PATH = "./database/other_data/process_output/consolidated_link/"

print("=> Processing text and link chunk data.")


def consolidate_chunks(chunk_path, chunktype):
    mframe = pd.DataFrame(None)
    for fn in os.listdir(chunk_path):
        if not fn.startswith(".") and os.path.isfile(os.path.join(chunk_path, fn)):
            rframe = pd.read_csv(os.path.join(chunk_path, fn), index_col=0)
            mframe = pd.concat([mframe, rframe], axis=0)
    mframe = mframe.reset_index()
    match chunktype:
        case "text":
            OUT_PATH = TEXT_OUT_PATH
            mframe = mframe.dropna(subset="job_text")
            mframe = mframe.drop_duplicates(subset="job_link")
            mframe = mframe.reset_index(drop=True)
            mframe.to_csv(
                os.path.join(OUT_PATH, f"consolidated_text_{date.today()}.csv"),
                index=False,
            )
        case "link":
            OUT_PATH = LINK_OUT_PATH
            mframe = mframe.dropna(subset="job_link")
            mframe = mframe.drop_duplicates(subset="job_link")
            mframe = mframe.reset_index(drop=True)
            mframe.to_csv(
                os.path.join(OUT_PATH, f"consolidated_link_{date.today()}.csv"),
                index=False,
            )
    return mframe


# Check if chunk consolidation exists for current date (TEXT)
if os.path.isfile(os.path.join(TEXT_OUT_PATH, f"consolidated_text_{date.today()}.csv")):
    TEXT_FRAME = pd.read_csv(
        os.path.join(TEXT_OUT_PATH, f"consolidated_text_{date.today()}.csv"),
        index_col=0,
    )
    TEXT_FRAME = TEXT_FRAME.reset_index()
else:
    TEXT_FRAME = consolidate_chunks("./webscraper/textscraper_chunks.nosync", "text")


# Check if chunk consolidation exists for current date (LINK)
if os.path.isfile(os.path.join(LINK_OUT_PATH, f"consolidated_link_{date.today()}.csv")):
    LINK_FRAME = pd.read_csv(
        os.path.join(LINK_OUT_PATH, f"consolidated_link_{date.today()}.csv"),
        index_col=0,
    )
    LINK_FRAME = LINK_FRAME.reset_index()
else:
    LINK_FRAME = consolidate_chunks("./webscraper/webscraper_chunks.nosync", "link")

# Process joined frame
main_frame = TEXT_FRAME.merge(LINK_FRAME, how="left", on="job_link")
main_frame["min_exp_years"] = (
    main_frame["min_exp"].str.split(" ").str[0].replace({"None": 0})
)
main_frame.pop("min_exp")
main_frame["last_posted_date"] = (
    main_frame["last_posted_date"]
    .str.split(" ")
    .apply(lambda x: datetime.strptime(" ".join(x[1:]), "%d %b %Y"))
)

# Check for existing frame and concat. Else output
DATA_PATH = os.path.join(os.getcwd(), "database/other_data/TEXTDATA.csv")
if os.path.isfile(DATA_PATH):
    EXISTING_FRAME = pd.read_csv(DATA_PATH, index_col=0)
    EXISTING_FRAME = EXISTING_FRAME.reset_index()
    RESULT_FRAME = pd.concat([main_frame, EXISTING_FRAME], axis=0)
else:
    RESULT_FRAME = main_frame
RESULT_FRAME = RESULT_FRAME.drop_duplicates(subset="job_link")
DROP_COLS = [x for x in list(RESULT_FRAME.columns.values) if "Unnamed" in x]
RESULT_FRAME = RESULT_FRAME.drop(DROP_COLS, axis=1)
RESULT_FRAME.to_csv(DATA_PATH, index=False)
print(RESULT_FRAME.columns.values)

print(f"=> TEXTDATA file created with {RESULT_FRAME.shape[0]} entries.")
