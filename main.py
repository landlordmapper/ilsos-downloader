import logging
import os
from pathlib import Path

from utils import process_dataset

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

DATASETS: list[dict] = [
    {
        "name": "Illinois Corporations Bulk Data - Master",
        "id": "cdxallmst",
        "url": "https://www.ilsos.gov/data/bs/cdxallmst.zip",
    },
    {
        "name": "Illinois Corporations Bulk Data - Company Name",
        "id": "cdxallnam",
        "url": "https://www.ilsos.gov/data/bs/cdxallnam.zip",
    },
    {
        "name": "Illinois Corporations Bulk Data - Agent",
        "id": "cdxallagt",
        "url": "https://www.ilsos.gov/data/bs/cdxallagt.zip",
    },
    {
        "name": "Illinois Corporations Bulk Data - Assumed/Old Names",
        "id": "cdxallaon",
        "url": "https://www.ilsos.gov/data/bs/cdxallaon.zip",
    },
    {
        "name": "Illinois LLC Bulk Data - Master",
        "id": "llcallmst",
        "url": "https://www.ilsos.gov/data/bs/llcallmst.zip",
    },
    {
        "name": "Illinois LLC Bulk Data - Company Name",
        "id": "llcallnam",
        "url": "https://www.ilsos.gov/data/bs/llcallnam.zip",
    },
    {
        "name": "Illinois LLC Bulk Data - Agent",
        "id": "llcallagt",
        "url": "https://www.ilsos.gov/data/bs/llcallagt.zip",
    },
    {
        "name": "Illinois LLC Bulk Data - Old Names",
        "id": "llcallold",
        "url": "https://www.ilsos.gov/data/bs/llcallold.zip",
    },
    {
        "name": "Illinois LLC Bulk Data - Managers",
        "id": "llcallmgr",
        "url": "https://www.ilsos.gov/data/bs/llcallmgr.zip",
    },
]


def main():
    out_path: Path = Path(os.path.join(os.path.curdir, "data"))
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # Process datasets
    for d in DATASETS:
        try:
            process_dataset(d, out_path)
        except Exception as e:
            logger.exception(
                "Failed processing %s (%s): %s", d.get("name"), d.get("id"), e
            )

    logger.info("Done.")


if __name__ == "__main__":
    main()

