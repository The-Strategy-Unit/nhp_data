"""Extract OPA data for model"""

import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from model_data.helpers import add_tretspef_column, get_spark


def extract_opa_data(
    save_path: str, fyear: int, spark: SparkSession = get_spark()
) -> None:
    """Extract OPA data

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
        .withColumn("tretspef_raw", F.col("tretspef"))
        .withColumn("is_wla", F.lit(True))
    )

    inequalities = (
        spark.read.parquet("/Volumes/nhp/inputs_data/files/dev/inequalities.parquet")
        .filter(F.col("fyear") == fyear)
        .select("provider", "sushrg_trimmed")
        .withColumnRenamed("provider", "dataset")
        .distinct()
    )

    # We don't want to keep sushrg_trimmed and imd_quintile if not in inequalities parquet file
    opa_collapse = (
        opa.join(inequalities, how="anti", on=["dataset", "sushrg_trimmed"])
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
        inequalities, how="semi", on=["dataset", "sushrg_trimmed"]
    )

    opa = DataFrame.unionByName(opa_collapse, opa_dont_collapse)

    opa = add_tretspef_column(opa)

    (
        opa.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/op")
    )


if __name__ == "__main__":
    path = sys.argv[1]
    fyear = int(sys.argv[2])

    extract_opa_data(path, fyear)
