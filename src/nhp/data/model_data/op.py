"""Extract OP data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.model_data.helpers import get_spark


def extract(save_path: str, fyear: int, spark: SparkSession = get_spark()) -> None:
    """Extract OP data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    opa = (
        spark.read.table("opa")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("is_wla", F.lit(True))
    )

    inequalities = (
        spark.read.table("inequalities")
        .filter(F.col("fyear") == fyear)
        .select("icb", "provider", "sushrg_trimmed")
        .withColumnRenamed("provider", "dataset")
        .distinct()
    )

    # We don't want to keep sushrg_trimmed and imd_quintile if not in inequalities
    opa_collapse = (
        opa.join(inequalities, how="anti", on=["icb", "dataset", "sushrg_trimmed"])
        .withColumn("sushrg_trimmed", F.lit(None))
        .withColumn("imd_quintile", F.lit(None))
        .groupBy(opa.drop("index", "attendances", "tele_attendances").columns)
        .agg(
            F.sum("attendances").alias("attendances"),
            F.sum("tele_attendances").alias("tele_attendances"),
            F.min("index").alias("index"),
        )
    )

    opa_dont_collapse = opa.join(
        inequalities, how="semi", on=["icb", "dataset", "sushrg_trimmed"]
    )

    opa = DataFrame.unionByName(opa_collapse, opa_dont_collapse)

    (
        opa.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/op")
    )


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])
    extract(path, fyear)
