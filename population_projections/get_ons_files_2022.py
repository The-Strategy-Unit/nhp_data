"""2022 based Population Projections download scripts

This module is used to download and extract the National and Subnational Population Projections
from the ONS website, ready to be used in our data processing steps.
"""

import io
import os
import re
import shutil
import time
from urllib.parse import urljoin
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup

ONS_URL = "https://www.ons.gov.uk"
BASE_URL = urljoin(
    ONS_URL,
    "/".join(
        [
            "peoplepopulationandcommunity",
            "populationandmigration",
            "populationprojections",
            "datasets",
            "",  # keep empty string to properly form urls
        ]
    ),
)


def get_snpp_uris(path: str) -> list[str]:
    """Get subnational population projection (SNPP) file URIs

    finds the URIs for the SNPP files to download from the ONS website.

    :param path: which page on the ONS site to load from (combines with BASE_URL)
    :type path: str
    :return: a list of the URIs for the files
    :rtype: list[str]
    """
    url = urljoin(BASE_URL, path)
    resp = requests.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    matches = soup.find_all(
        "a",
        attrs={
            "aria-label": lambda x: x
            and "2022-based" in x
            and re.match("^Download (Population|Birth) projections", x)
        },
    )

    return [urljoin(ONS_URL, i["href"]) for i in matches]


def snpp_uri_variant_match(uri: str) -> str | None:
    """Get the name of the variant from a URI

    :param uri: the URI for an SNPP file
    :type uri: str
    :return: the variant name we use in the NHP model
    :rtype: str | None
    """
    file = re.sub("(.*/2022snpp(populationsyoa|births))|\.zip$", "", uri)

    match file:
        case "migcat23":
            return "migration_category"
        case "migcat":
            return None
        case "5yr" | "5year":
            return "var_proj_5_year_migration"
        case "lowmig":
            return "var_proj_low_intl_migration"
        case "highmig":
            return "var_proj_high_intl_migration"
        case "10yr" | "10year":
            return "var_proj_10_year_migration"
        case "zeronet":
            return "var_proj_zero_net_migration"
        case _:
            return None


def extract_snpp_zip(uri: str, output_dir: str) -> str:
    """Download and extract an SNPP zip file

    :param uri: the URI to the SNPP file
    :type uri: str
    :param output_dir: the path where we want to save the files to
    :type output_dir: str
    :raises Exception: if the URI does not match to demographics/births files
    :return: path to where we have saved the files to
    :rtype: str
    """
    variant = snpp_uri_variant_match(uri)
    if not variant:
        return None

    if "populationsyoa" in uri:
        name, dir_name = "Population", "demographics"
    elif "births" in uri:
        name, dir_name = "Births", "births"
    else:
        raise Exception("Unexpected uri")

    while True:
        response = requests.get(uri)
        if response.status_code == 429:
            time.sleep(30)
        else:
            response.raise_for_status()
            break

    # add a back off to any future http calls
    time.sleep(1)

    path = os.path.join(output_dir, dir_name, variant)
    os.makedirs(output_dir, exist_ok=True)

    with ZipFile(io.BytesIO(response.content)) as z:
        for s in ["males", "females"]:
            z.extract(f"2022 SNPP {name} {s}.csv", path)
            shutil.move(
                os.path.join(path, f"2022 SNPP {name} {s}.csv"),
                os.path.join(path, f"{s}.csv"),
            )

    # for births, combinbe into a single "persons.csv" file
    if dir_name == "births":
        files = [os.path.join(path, f"{i}.csv") for i in ["females", "males"]]

        (
            pd.concat([pd.read_csv(i) for i in files])
            .drop(columns=["AREA_NAME", "COMPONENT", "SEX"])
            .groupby(["AREA_CODE", "AGE_GROUP"], as_index=False)
            .sum()
            .query("AGE_GROUP != 'All ages'")
            .to_csv(os.path.join(path, "persons.csv"), index=False)
        )

        for i in files:
            os.remove(i)

    return path


def get_npp_uri(path: str) -> str:
    """Get National Population Projection (NPP) file URI

    :param path: which page on the ONS site to load from (combines with BASE_URL)
    :type path: str
    :return: the URI to the NPP zip file to download
    :rtype: str
    """
    url = urljoin(BASE_URL, path)
    resp = requests.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    pattern = "Download Zipped population projections data files, .*: 2022 based in zip format"
    match = soup.find(
        "a",
        attrs={"aria-label": lambda x: x and re.match(pattern, x)},
    )

    return urljoin(ONS_URL, match["href"])


def extract_npp_zip(uri: str, dir_in_zip: str, output_dir: str) -> str:
    """Download and extract the NPP zip file

    :param uri: the URI to the NPP zip file
    :type uri: str
    :param dir_in_zip: the directory in the zip that contains the files
    :type dir_in_zip: str
    :param output_dir: the path where we want to save the files to
    :type output_dir: str
    :return: _description_
    :rtype: str
    """

    while True:
        response = requests.get(uri)
        if response.status_code == 429:
            time.sleep(30)
        else:
            response.raise_for_status()
            break

    with ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(output_dir)

    npp_path = os.path.join(output_dir, "npp")
    # if the npp_path already exists, overwrite it
    if os.path.exists(npp_path):
        shutil.rmtree(npp_path)
    shutil.move(os.path.join(output_dir, dir_in_zip), npp_path)

    for i in os.listdir(npp_path):
        j = i.replace("uk_", "").replace("_machine_readable", "")
        shutil.move(os.path.join(npp_path, i), os.path.join(npp_path, j))

    return npp_path


def get_2022_population_files(output_dir: str) -> None:
    """Download the 2022 population projection files

    :param output_dir: the path where we want to save the files to
    :type output_dir: str
    """
    snpp_uris = [
        *get_snpp_uris("localauthoritiesinenglandz1"),
        *get_snpp_uris("birthsbyageofmotherz3"),
    ]

    for uri in snpp_uris:
        extract_snpp_zip(uri, output_dir)

    npp_uri = get_npp_uri("z1zippedpopulationprojectionsdatafilesuk")
    extract_npp_zip(npp_uri, "uk", output_dir)


def main():
    """main method"""
    import sys

    path = sys.argv[1]
    projection_year = sys.argv[2]

    output_dir = os.path.join(path, f"{projection_year}-projections")

    if not os.path.exists(output_dir):
        get_2022_population_files(output_dir)


if __name__ == "__main__":
    main()
