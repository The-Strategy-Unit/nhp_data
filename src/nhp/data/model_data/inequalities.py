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

    inequalities = (
        spark.read.table("nhp.default.inequalities")
        .filter(F.col("fyear") == fyear)
        .withColumn("fyear", F.lit(fyear_converted))
        .withColumnRenamed("provider", "dataset")
    )

    (
        inequalities.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/inequalities")
    )

    # Write empty parquet files for missing providers
    providers = (
        spark.read.table("nhp.default.apc")
        .filter(F.col("fyear") == fyear)
        .select("provider")
        .distinct()
    )
    missing_providers = (
        providers.withColumnRenamed("provider", "dataset")
        .join(inequalities.select("dataset").distinct(), on="dataset", how="left_anti")
        .withColumn("fyear", F.lit(fyear_converted))
    )
    dummy_missing = missing_providers.join(
        spark.createDataFrame([], inequalities.schema),
        on=["fyear", "dataset"],
        how="left",
    )
    (
        dummy_missing.repartition(1)
        .write.mode("append")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/inequalities")
    )


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])
    extract(path, fyear)
