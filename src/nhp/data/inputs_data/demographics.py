"""Demographics/Birth/Catchments"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.save_parquet import save_parquet
from nhp.data.table_names import table_names


def extract_demographic_factors(path: str, spark: SparkSession) -> None:
    """Extract Demographic Factors data

    :param path: the path to save the extracted data
    :type path: str
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    df = spark.read.table(table_names.reference_population_provider_demographics)
    save_parquet(df, f"{path}/demographic_factors")


def extract_birth_factors(path: str, spark: SparkSession) -> None:
    """Extract Birth Factors data

    :param path: the path to save the extracted data
    :type path: str
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    df = spark.read.table(table_names.reference_population_provider_births)
    save_parquet(df, f"{path}/birth_factors")


def extract_provider_catchments(path: str, spark: SparkSession) -> None:
    """Extract Provider Catchments data

    :param path: the path to save the extracted data
    :type path: str
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    df = (
        spark.read.table(table_names.reference_provider_lad23_splits)
        .filter(F.col("fyear") >= 202324)
        .orderBy("fyear", "provider", "lad23cd", "sex", "age")
    )
    save_parquet(df, f"{path}/provider_catchments")


def main() -> None:
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"

    spark = get_spark()
    extract_demographic_factors(path, spark)
    extract_birth_factors(path, spark)
    extract_provider_catchments(path, spark)
