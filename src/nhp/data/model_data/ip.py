"""Extract IP data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.model_data.helpers import get_spark
from nhp.data.table_names import table_names


def extract(save_path: str, fyear: int, spark: SparkSession) -> None:
    """Extract IP (+mitigators) data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    apc = (
        spark.read.table(table_names.default_apc)
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("epikey", "rn")
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("sex", F.col("sex").cast("int"))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
    )

    (
        apc.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/ip")
    )

    for k, v in [
        ("activity_avoidance", "activity_avoidance"),
        ("efficiencies", "efficiency"),
    ]:
        (
            spark.read.table(table_names.default.apc_mitigators)
            .filter(F.col("type") == v)
            .filter(F.col("fyear") == fyear)
            .drop("type", "fyear")
            .withColumnRenamed("epikey", "rn")
            .withColumnRenamed("provider", "dataset")
            .join(apc, ["dataset", "rn"], "inner")
            .select("dataset", "fyear", "rn", "strategy", "sample_rate")
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy(["fyear", "dataset"])
            .parquet(f"{save_path}/ip_{k}_strategies")
        )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract(save_path, fyear, spark)
