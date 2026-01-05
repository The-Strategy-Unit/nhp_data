import io

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def download_arcgis_results(url: str, where="1=1", fields="*") -> pd.DataFrame:
    params = {
        "where": where,
        "outFields": fields,
        "f": "geojson",
        "resultRecordCount": 1000,
        "resultOffset": 0,
    }
    features = []
    while True:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        features.extend(data["features"])
        if (
            "properties" in data
            and "exceededTransferLimit" in data["properties"]
            and data["properties"]["exceededTransferLimit"]
        ):
            params["resultOffset"] = len(features)
        else:
            break

    return pd.DataFrame([i["properties"] for i in features])


def download_lsoa11_to_lsoa21_lookup() -> pd.DataFrame:
    url = "/".join(
        [
            "https://open-geography-portalx-ons.hub.arcgis.com",
            "api",
            "download",
            "v1",
            "items",
            "cbfe64cc03d74af982c1afec639bafd1",
            "csv",
        ]
    )

    result = requests.get(url, params={"layers": 0})
    result.raise_for_status()

    content = io.StringIO(result.content.decode("utf-8-sig"))

    df = pd.read_csv(content, usecols=["LSOA11CD", "LSOA21CD"])  # ty: ignore[no-matching-overload]
    df.columns = [col.lower() for col in df.columns]

    return df


def get_lsoa11_to_lsoa21_lookup(spark: SparkSession) -> DataFrame:
    table = table_names.reference_lsoa11_to_lsoa21
    if not spark.catalog.tableExists(table):
        df = download_lsoa11_to_lsoa21_lookup()
        spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_lsoa21_to_lad23_lookup() -> pd.DataFrame:
    lsoa21_to_lad23 = download_arcgis_results(
        "/".join(
            [
                "https://services1.arcgis.com",
                "ESMARspQHYMw9BZ9",
                "arcgis",
                "rest",
                "services",
                "LSOA21_WD23_LAD23_EW_LU",
                "FeatureServer",
                "0",
                "query",
            ]
        ),
        fields="LSOA21CD,LAD23CD",
    )

    lsoa21_to_lad23.columns = [col.lower() for col in lsoa21_to_lad23.columns]
    return lsoa21_to_lad23


def get_lsoa21_to_lad23(spark: SparkSession) -> DataFrame:
    table = table_names.reference_lsoa21_to_lad23
    if not spark.catalog.tableExists(table):
        df = create_lsoa21_to_lad23_lookup()

        spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_lsoa11_to_lad23_lookup(spark: SparkSession) -> DataFrame:
    lsoa11_to_lsoa21 = get_lsoa11_to_lsoa21_lookup(spark)
    lsoa21_to_lad23 = get_lsoa21_to_lad23(spark)

    df = (
        lsoa11_to_lsoa21.filter(F.col("lsoa11cd").startswith("E"))  # ty:ignore[missing-argument, invalid-argument-type]
        .join(lsoa21_to_lad23, "lsoa21cd")
        .select("lsoa11cd", "lad23cd")
        .distinct()
        # handle some specific edge cases:
        # these 4 LSOA11s map to multiple LAD23s, but geospatial analysis shows
        # that >99% of the area for each LSOA11 is within a single LAD23
        .filter(
            ~(
                ((F.col("lsoa11cd") == "E01008187") & (F.col("lad23cd") != "E08000037"))
                | (
                    (F.col("lsoa11cd") == "E01027506")
                    & (F.col("lad23cd") != "E06000057")
                )
                | (
                    (F.col("lsoa11cd") == "E01023964")
                    & (F.col("lad23cd") != "E07000241")
                )
                | (
                    (F.col("lsoa11cd") == "E01023508")
                    & (F.col("lad23cd") != "E07000242")
                )
            )
        )
    )

    # validate that this mapping is valid
    check = df.groupBy("lsoa11cd").count().filter(F.col("count") > 1).count()
    assert check == 0, "Some LSOA11s map to multiple LAD23s"
    assert df.count() == 32844, "Unexpected number of LSOA11 to LAD23 mappings"
    assert df.select("lad23cd").distinct().count() == 296, "Unexpected number of LAD23s"

    return df


def get_lsoa11_to_lad23_lookup(spark: SparkSession) -> DataFrame:
    table = table_names.reference_lsoa11_to_lad23
    if not spark.catalog.tableExists(table):
        df = create_lsoa11_to_lad23_lookup(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def main():
    spark = get_spark()

    # this will generate the other lsoa lookups as needed
    get_lsoa11_to_lad23_lookup(spark)
