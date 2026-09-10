"""Extract IP TPMAs data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names


def _extract_ip_tpmas(
    tpma_type: str, save_path: str, fyear: int, spark: SparkSession
) -> DataFrame:
    apc = spark.read.parquet(f"{save_path}/ip").filter(F.col("fyear") == fyear // 100)

    return (
        spark.read.table(table_names.default_apc_mitigators)
        .filter(F.col("type") == tpma_type)
        .filter(F.col("fyear") == fyear)
        .drop("type", "fyear")
        .withColumnRenamed("epikey", "rn")
        .withColumnRenamed("provider", "dataset")
        .join(apc, ["dataset", "rn"], "inner")
        .select("dataset", "fyear", "rn", "strategy", "sample_rate")
    )


@extract("ip_activity_avoidance_strategies")
def extract_ip_activity_avoidance_tpmas(
    save_path: str, fyear: int, spark: SparkSession
) -> DataFrame:
    """Extract IP activity avoidance TPMAs data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    return _extract_ip_tpmas("activity_avoidance", save_path, fyear, spark)


@extract("ip_efficiency_strategies")
def extract_ip_efficiency_tpmas(
    save_path: str, fyear: int, spark: SparkSession
) -> DataFrame:
    """Extract IP efficiency TPMAs data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    return _extract_ip_tpmas("efficiencies", save_path, fyear, spark)


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_ip_activity_avoidance_tpmas(save_path, fyear, spark)
    extract_ip_efficiency_tpmas(save_path, fyear, spark)
