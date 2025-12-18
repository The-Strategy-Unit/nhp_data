import io

import bs4
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.reference.lsoa_lookups import get_lsoa21_to_lad23
from nhp.data.table_names import table_names


def create_pop_by_lsoa21():
    BASE_URL = url = "https://www.ons.gov.uk"

    url = "/".join(
        [
            BASE_URL,
            "peoplepopulationandcommunity",
            "populationandmigration",
            "populationestimates",
            "datasets",
            "lowersuperoutputareamidyearpopulationestimates",
        ]
    )
    resp = requests.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    def get_pop_by_lsoa_file(file_title: str, years: list[int]) -> pd.DataFrame:
        def year_to_fyear(year):
            return year * 100 + (year + 1) % 100

        file_link = soup.find(
            "a", attrs={"aria-label": lambda x: x and file_title in x}
        )
        assert file_link is not None, f"could not find link for {file_title}"
        assert isinstance(file_link, bs4.Tag), "expected bs4.Tag"
        href = file_link["href"]

        url = f"{BASE_URL}/{href}"

        response = requests.get(url)
        response.raise_for_status()

        file = io.BytesIO(response.content)

        return pd.concat(
            [
                (
                    pd.read_excel(file, sheet_name=f"Mid-{year} LSOA 2021", skiprows=3)
                    .rename(columns={"LSOA 2021 Code": "lsoa21cd"})
                    .melt(
                        id_vars="lsoa21cd",
                        value_vars=[f"{s}{i}" for s in "FM" for i in range(0, 91)],
                        var_name="sex_age",
                        value_name="population",
                    )
                    .assign(
                        sex=lambda x: np.where(x["sex_age"].str[0] == "M", 1, 2),
                        age=lambda x: x["sex_age"].str[1:].astype("int"),
                        fyear=year_to_fyear(year),
                    )
                    .drop(columns=["sex_age"])
                )
                for year in years
            ]
        )

    df = pd.concat(
        [
            get_pop_by_lsoa_file(*f)  # ty: ignore[invalid-argument-type]
            for f in [
                ("Mid-2011 to mid-2014", [2011, 2012, 2013, 2014]),
                ("Mid-2015 to mid-2018", [2015, 2016, 2017, 2018]),
                ("Mid-2019 to mid-2022", [2019, 2020, 2021]),
                ("Mid-2022 revised (Nov 2025) to mid-2024", [2022, 2023, 2024]),
            ]
        ]
    )

    return df[["fyear", "lsoa21cd", "sex", "age", "population"]]


def get_pop_by_lsoa21(spark: SparkSession) -> DataFrame:
    table = table_names.reference_pop_by_lsoa21
    if not spark.catalog.tableExists(table):
        pop_by_lsoa21 = create_pop_by_lsoa21()
        spark.createDataFrame(pop_by_lsoa21).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_pop_by_lad23(spark: SparkSession) -> DataFrame:
    lsoa21_to_lad23 = get_lsoa21_to_lad23(spark)
    pop_by_lsoa21 = get_pop_by_lsoa21(spark)

    return (
        pop_by_lsoa21.filter(F.col("lsoa21cd").startswith("E"))
        .join(lsoa21_to_lad23, "lsoa21cd")
        .groupBy("fyear", "lad23cd", "age", "sex")
        .agg(F.sum("population").alias("population"))
    )


def get_pop_by_lad23(spark: SparkSession) -> DataFrame:
    table = table_names.reference_pop_by_lad23
    if not spark.catalog.tableExists(table):
        df = create_pop_by_lad23(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def main():
    spark = get_spark()

    get_pop_by_lad23(spark)
