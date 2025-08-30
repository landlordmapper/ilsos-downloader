import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from curl_cffi import requests
from tenacity import retry, stop_after_attempt
from tenacity.retry import retry_if_exception
from tenacity.wait import wait_exponential

# configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# types
FixedWidthSchema = list[tuple[str, int, int]]
# constants
ILSOS_SCHEMA: dict[str, FixedWidthSchema] = {
    "cdxallmst": [
        ("file_number", 0, 8),
        ("incorp_date", 8, 8),
        ("extended_date", 16, 8),
        ("state_code", 24, 2),
        ("corp_intent", 26, 3),
        ("status", 29, 2),
        ("type_corp", 31, 1),
        ("trans_date", 32, 8),
        ("pres_name_addr", 40, 60),
        ("sec_name_addr", 100, 60),
    ],
    "cdxallnam": [("file_number", 0, 8), ("name", 8, 189)],
    "cdxallagt": [
        ("file_number", 0, 8),
        ("agent_name", 8, 60),
        ("agent_street", 68, 45),
        ("agent_city", 113, 30),
        ("agent_change_date", 143, 8),
        ("agent_code", 151, 1),
        ("agent_zip", 152, 9),
        ("agent_county_code", 161, 3),
    ],
    "cdxallaon": [
        ("file_number", 0, 8),
        ("cancel_date", 8, 8),
        ("assumed_curr_date", 16, 8),
        ("assumed_old_ind", 24, 1),
        ("assumed_old_date", 25, 8),
        ("assumed_old_name", 33, 189),
    ],
    "llcallmst": [
        ("file_number", 0, 8),
        ("purpose_code", 8, 6),
        ("status_code", 14, 2),
        ("status_date", 16, 8),
        ("organized_date", 24, 8),
        ("dissolution_date", 32, 8),
        ("management_type", 40, 1),
        ("juris_organized", 41, 2),
        ("records_off_street", 43, 45),
        ("records_off_city", 88, 30),
        ("records_off_zip", 118, 9),
        ("records_off_juris", 127, 2),
        ("assumed_in", 129, 1),
        ("old_ind", 130, 1),
        ("provisions_ind", 131, 1),
        ("opt_ind", 132, 1),
        ("series_ind", 133, 1),
        ("uap_ind", 134, 1),
        ("l3c_ind", 135, 1),
    ],
    "llcallnam": [("file_number", 0, 8), ("name", 8, 120)],
    "llcallagt": [
        ("file_number", 0, 8),
        ("agent_code", 8, 1),
        ("agent_name", 9, 60),
        ("agent_street", 69, 45),
        ("agent_city", 114, 30),
        ("agent_zip", 144, 9),
        ("agent_county_code", 153, 3),
        ("agent_change_date", 156, 8),
    ],
    "llcallold": [
        ("file_number", 0, 8),
        ("old_date_filed", 8, 8),
        ("llc_name", 16, 120),
        ("series_nbr", 136, 3),
    ],
    "llcallmgr": [
        ("file_number", 0, 8),
        ("mm_name", 8, 60),
        ("mm_street", 68, 45),
        ("mm_city", 113, 30),
        ("mm_juris", 143, 2),
        ("mm_zip", 145, 9),
        ("mm_file_date", 154, 8),
        ("mm_type_code", 162, 1),
    ],
}


# request/retry handlers
def is_retryable_http_error(e: BaseException) -> bool:
    """
    Retry on transient network/server errors from curl_cffi.requests.
    """
    # network/timeout
    if isinstance(
        e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    ):
        return True
    # 5xx after raise_for_status()
    if isinstance(e, requests.exceptions.HTTPError):
        resp = getattr(e, "response", None)
        if resp is not None and 500 <= resp.status_code < 600:
            return True
    return False


retry_handler = retry(
    # Only retry on specific, transient errors
    retry=retry_if_exception(is_retryable_http_error),
    # Wait 2^x * 1 second between each retry, starting with 4s, max 10s
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)


@retry_handler
def get_zip_content(url: str, **kwargs) -> requests.Response:
    """
    Fetches zip file content from a URL using browser impersonation to bypass advanced anti-bot measures.
    """
    response = requests.get(url, impersonate="chrome110", timeout=180, **kwargs)
    response.raise_for_status()
    return response


def parse_fixed_width(txt_content: str, schema: FixedWidthSchema) -> pd.DataFrame:
    """
    Converts fixed-width text content to a DataFrame based on schema.
    """
    # Split content into lines and skip mainframe header/footer
    lines = txt_content.strip().splitlines()[1:-1]
    processed_rows: list[dict[str, Any]] = []
    for line in lines:
        # character cleaning
        clean_line = (
            line.replace("İ", "[")
            .replace("¨", "]")
            .replace("¬", "^")
            .replace("\x00", "")
        )
        row = {}
        for col_name, start, length in schema:
            end = start + length
            value = clean_line[start:end].strip()
            row[col_name] = value
        processed_rows.append(row)
    return pd.DataFrame(processed_rows)


def process_dataset(dataset: dict[str, str], out_path: Path) -> None:
    url = dataset["url"]
    id = dataset["id"]
    # Use a temporary directory for all intermediate file operations.
    # It will be automatically deleted upon exiting the 'with' block.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path: Path = Path(temp_dir)
        zip_path: Path = Path(os.path.join(temp_path, f"{id}.zip"))
        # download zip file using the impersonating HTTP client
        logger.info(
            f"Fetching fixed-width data from zip file for dataset '{dataset['name']}'. URL: {url}"
        )
        zip_content: requests.Response = get_zip_content(url, stream=True)
        logger.info("Content fetched successfully. Unzipping...")
        with open(zip_path, "wb") as f:
            for chunk in zip_content.iter_content(chunk_size=8192):
                f.write(chunk)
        # unzip the archive to get the text content in memory
        txt_content: str = ""
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            txt_filename: str = next(
                name for name in zip_ref.namelist() if name.lower().endswith(".txt")
            )
            with zip_ref.open(txt_filename) as txt_file:
                # read and decode using the correct mainframe encoding
                txt_content = txt_file.read().decode("ISO-8859-9")
        logger.info(
            "Dataset successfully unzipped and decoded. Parsing fixed width text files..."
        )
        # process the text content directly into a DataFrame
        out_file_path: Path = Path(os.path.join(out_path, f"{id}.csv"))
        df: pd.DataFrame = parse_fixed_width(txt_content, ILSOS_SCHEMA[id])
        df.to_csv(out_file_path, index=False)
        logger.info(
            f"Successfully saved {dataset['name']} to {out_path / f'{id}.csv'}."
        )
