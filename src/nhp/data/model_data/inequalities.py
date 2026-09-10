"""Extract inequalities data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names


@extract("inequalities", check_for_nulls=False)
def extract_inequalities(save_path: str, fyear: int, spark: SparkSession) -> DataFrame:
    """Extract inequalities data for model

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """

    fyear_converted = fyear // 100

    inequalities = spark.read.table(table_names.default_inequalities).filter(
        F.col("fyear") == fyear
    )

    # handle providers with no inequalities data
    providers = (
        spark.read.table(table_names.default_apc)
        .filter(F.col("fyear") == fyear)
        .select("provider")
        .distinct()
    )

    return (
        inequalities.join(providers, on="provider", how="right")
        .withColumn("fyear", F.lit(fyear_converted))
        .withColumnRenamed("provider", "dataset")
    )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_inequalities(save_path, fyear, spark)
