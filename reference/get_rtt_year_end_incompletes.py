import sys
from datetime import date
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup


def _base_page(year: int) -> str:
    # fyear as a string yyyy-zz
    fyear = f"{year}-{(year + 1) % 100}"

    path = ["statistics"]
    if year > 2014:
        path.append("statistical-work-areas")
    if year != 2014:
        path.append("rtt-waiting-times")

    return f"https://www.england.nhs.uk/{'/'.join(path)}/rtt-data-{fyear}/"


def _clean_tretspef(t):
    if "X01" in t:
        return "Other"
    elif "X02" in t:
        return "Other (Medical)"
    elif "X03" in t:
        return "Other"  # "Other (Mental Health)"
    elif "X04" in t:
        return "Other"  # "Other (Paediatric)"
    elif "X05" in t:
        return "Other (Surgical)"
    elif "X06" in t:
        return "Other"
    elif "999" in t:
        return "Total"
    else:
        return t.removeprefix("C_").removeprefix("IP")


def _get_march_incomplete(year: int) -> pd.DataFrame:
    url = _base_page(year)

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    mar_url = next(
        a["href"] for a in soup.find_all("a") if "Incomplete Provider Mar" in a.text
    )

    response = requests.get(mar_url)
    response.raise_for_status()

    provider_code_col = "Provider Code" if year > 2012 else "Org Code"

    df = (
        pd.read_excel(BytesIO(response.content), skiprows=13, sheet_name="Provider")
        .assign(date=date(year + 1, 3, 31))
        .rename(
            columns={
                provider_code_col: "procode3",
                "Treatment Function Code": "tretspef",
                "Total number of incomplete pathways": "n",
            }
        )
    )

    df["tretspef"] = df["tretspef"].apply(_clean_tretspef)

    return (
        df.query("tretspef != 'total'")
        .groupby(["date", "procode3", "tretspef"], as_index=False)["n"]
        .sum()
    )


def get_rtt(save_path: str, start_year: int, end_year: int) -> None:
    df = pd.concat([_get_march_incomplete(i) for i in range(start_year, end_year)])

    df.to_parquet(f"{save_path}/rtt_year_end_incompletes.parquet")


def init() -> None:
    path = sys.argv[1]
    start_year = int(sys.argv[2])
    end_year = int(sys.argv[3])
    get_rtt(path, start_year, end_year)


if __name__ == "__main__":
    init()
