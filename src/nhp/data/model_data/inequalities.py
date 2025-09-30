"""Extract inequalities data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.model_data.helpers import get_spark


def extract(save_path: str, fyear: int, spark: SparkSession = get_spark()) -> None:
    """Extract inequalities data for model

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    fyear_converted = fyear // 100

    inequalities = spark.read.table("nhp.default.inequalities").filter(
        F.col("fyear") == fyear
    )

    # handle providers with no inequalities data
    providers = (
        spark.read.table("nhp.default.apc")
        .filter(F.col("fyear") == fyear)
        .select("provider")
        .distinct()
    )

    inequalities_with_missing_providers = (
        inequalities.join(providers, on="provider", how="right")
        .withColumn("fyear", F.lit(fyear_converted))
        .withColumnRenamed("provider", "dataset")
    )

    (
        inequalities_with_missing_providers.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/inequalities")
    )


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])
    extract(path, fyear)
