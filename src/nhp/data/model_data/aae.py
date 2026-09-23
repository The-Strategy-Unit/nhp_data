"""Extract A&E data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names

AAE_EXCLUDE_COLS = {"aedepttype", "acuity", "pod", "attendance_category", "icb"}


@extract("aae", check_for_nulls=True, exclude_cols=AAE_EXCLUDE_COLS)
def extract_aae(save_path: str, fyear: int, spark: SparkSession) -> DataFrame:
    """Extract A&E data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    return (
        spark.read.table(table_names.default_ecds)
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .fillna({"sitetret": "unknown"})
    )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_aae(save_path, fyear, spark)
