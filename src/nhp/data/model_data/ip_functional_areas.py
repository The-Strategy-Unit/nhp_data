"""Extract IP functional areas data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.functional_areas.ip.beds import get_ip_functional_area_beds
from nhp.data.model_data.functional_areas.ip.procedures import (
    get_ip_functional_areas_procedures,
)
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names


@extract("ip_functional_areas_beds", check_for_nulls=False)
def extract_ip_functional_areas(
    save_path: str, fyear: int, spark: SparkSession
) -> DataFrame:
    apc = spark.read.parquet(f"{save_path}/ip").filter(F.col("fyear") == fyear // 100)
    return get_ip_functional_area_beds(apc, spark)


@extract("ip_functional_areas_procedures", check_for_nulls=False)
def extract_ip_functional_areas_procedures(
    save_path: str, fyear: int, spark: SparkSession
) -> DataFrame:
    apc = spark.read.parquet(f"{save_path}/ip").filter(F.col("fyear") == fyear // 100)
    return get_ip_functional_areas_procedures(apc, spark)


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_ip_functional_areas(save_path, fyear, spark)
    extract_ip_functional_areas_procedures(save_path, fyear, spark)
