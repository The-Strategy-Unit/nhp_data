"""Create ICB Catchments"""

from collections import defaultdict

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark


def get_icb_catchments(spark: SparkSession) -> DataFrame:
    """Get ICB Catchments

    Uses a lookup of ICB<->LSOA<->LAD to split each Local Authority proportionally into ICBs based
    on the amount of LSOAs each ICB has in that LAD.

    :param spark: The Spark context
    :type spark: SparkSession
    """

    url = "/".join(
        [
            "https://services1.arcgis.com",
            "ESMARspQHYMw9BZ9",
            "arcgis",
            "rest",
            "services",
            "LSOA21_SICBL23_ICB23_LAD23_EN_LU",
            "FeatureServer",
            "0",
            "query",
        ]
    )

    col_lsoa = "LSOA21CD"
    col_icb = "ICB23CDH"
    col_lad = "LAD23CD"

    params = {
        "where": "1=1",
        "outFields": f"{col_lsoa},{col_icb},{col_lad}",
        "returnGeometry": "false",
        "outSR": 4326,
        "f": "json",
        "resultOffset": 0,
        "resultRecordCount": 2000,
    }

    all_features = []
    while True:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        params["resultOffset"] += len(features)

    lad_icb_lsoa_counts = defaultdict(lambda: defaultdict(lambda: 0))
    icb_la_pcnts = []

    for feature in all_features:
        i = feature["attributes"]
        lad_icb_lsoa_counts[i[col_lad]][i[col_icb]] += 1

    for lad, v in lad_icb_lsoa_counts.items():
        t = sum(v.values())
        for icb, n in v.items():
            icb_la_pcnts.append((icb, lad, n / t))

    return spark.createDataFrame(
        icb_la_pcnts, "icb: string, area_code: string, pcnt: double"
    )


def create_icb_catchments(spark: SparkSession) -> None:
    """Create ICB Catchments

    :param spark: The Spark context
    :type spark: SparkSession
    """

    df = get_icb_catchments(spark)
    df.write.mode("overwrite").saveAsTable("icb_catchments")


def main():
    spark = get_spark("reference")
    create_icb_catchments(spark)
