"""Get the organisation types for trusts from the Estates Returns Information Collection (ERIC).

I have not found a better place for getting a mappings for trust to type, where type is something
like "acute", "community", "ambulance", etc. The ERIC returns are about the only place that seems to
have this.

This script grabs the ERIC returns over a number of years and finds the most recent entry for each
trust, giving us a mapping from trust org code to trust type.
"""

import re
import sys
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

from nhp.data.table_names import table_names

BASE_URL = "https://digital.nhs.uk"


def year_to_fyear(year: str) -> int:
    y = int(year)
    return y * 100 + (y + 1) % 100


def get_eric_links():
    response = requests.get(
        f"{BASE_URL}/data-and-information/publications/statistical/estates-returns-information-collection"
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.content)

    pattern = r"Estates Returns? Information Collection(.*Summary page and dataset for ERIC \d{4}/\d{2}|.* England, \d{4}-\d{2})"

    div = soup.find("div", id="past-publications")
    assert div is not None, "could not find past publications div"

    links = [a.attrs["href"] for a in div.find_all("a") if re.match(pattern, a.text)]

    # ensure the links are sorted in year order
    return sorted([(year_to_fyear(i[-7:-3]), i) for i in links])


def get_eric_trust_data(year: int, link: str) -> pd.DataFrame:
    response = requests.get(f"{BASE_URL}/{link}")
    response.raise_for_status()

    soup = BeautifulSoup(response.content)
    div = soup.find("div", id="resources")
    assert div is not None, "could not find resources div"

    uri = next(a.attrs["href"] for a in div.find_all("a") if "Trust" in a.text)

    response = requests.get(uri)
    response.raise_for_status()

    # more recent years are UTF-8, but earlier years are Latin alphabet No. 1
    try:
        content = response.content.decode("UTF-8")
    except UnicodeDecodeError:
        content = response.content.decode("ISO-8859-1")

    return (
        # some earlier years have some columns headers spanning over 2 rows
        pd.read_csv(StringIO(content), skiprows=1 if content[0] == "," else 0)
        .rename(columns={"Trust Code": "org_code", "Trust Type": "org_type"})
        .assign(year=year)[["year", "org_code", "org_type"]]
    )


def get_trust_types() -> pd.DataFrame:
    df = pd.concat(
        [get_eric_trust_data(*i) for i in get_eric_links()],
        ignore_index=True,
    )

    return df.loc[df.groupby("org_code")["year"].idxmax()]


def create_trust_types_parquet(path: str) -> None:
    get_trust_types().to_parquet(table_names.reference_trust_types)


def init() -> None:
    path = sys.argv[1]
    create_trust_types_parquet(path)


if __name__ == "__main__":
    init()
