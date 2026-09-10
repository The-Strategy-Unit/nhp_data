"""Extract OP data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names

OPA_EXCLUDE_COLS = {"imd_quintile", "sushrg_trimmed", "icb"}


@extract("op", True, OPA_EXCLUDE_COLS)
def extract_op(save_path: str, fyear: int, spark: SparkSession) -> DataFrame:
    """Extract OP data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """

    opa = (
        spark.read.table(table_names.default_opa)
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("is_wla", F.lit(True))
        .fillna({"sitetret": "unknown"})
    )

    inequalities = (
        spark.read.table(table_names.default_inequalities)
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

    return opa_collapse.unionByName(opa_dont_collapse)


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_op(save_path, fyear, spark)
