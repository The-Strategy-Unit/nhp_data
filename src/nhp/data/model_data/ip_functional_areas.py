"""Extract IP functional areas data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.functional_areas.ip.beds import get_ip_functional_area_beds
from nhp.data.model_data.functional_areas.ip.procedures import (
    get_ip_functional_areas_procedures,
)
from nhp.data.table_names import table_names


def extract(save_path: str, fyear: int, spark: SparkSession) -> None:
    """Extract IP functional areas data

    :param spark: the spark session to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    apc = spark.read.parquet(f"{save_path}/ip").filter(F.col("fyear") == fyear // 100)

    (
        get_ip_functional_area_beds(apc, spark)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/ip_functional_areas_beds")
    )

    (
        get_ip_functional_areas_procedures(apc, spark)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/ip_functional_areas_procedures")
    )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract(save_path, fyear, spark)
