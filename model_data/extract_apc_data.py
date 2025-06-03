"""Helper methods/tables"""

import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from model_data.helpers import add_tretspef_column, get_spark


def extract_apc_data(
    save_path: str, fyear: int, spark: SparkSession = get_spark()
) -> None:
    """Extract APC (+mitigators) data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    apc = (
        spark.read.table("apc")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("epikey", "rn")
        .withColumnRenamed("provider", "dataset")
        .withColumn("tretspef_raw", F.col("tretspef"))
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("sex", F.col("sex").cast("int"))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
    )

    apc = add_tretspef_column(apc)

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
            spark.read.table("apc_mitigators")
            .filter(F.col("type") == v)
            .drop("type")
            .withColumnRenamed("epikey", "rn")
            .join(apc, "rn", "inner")
            .select("dataset", "fyear", "rn", "strategy", "sample_rate")
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy(["fyear", "dataset"])
            .parquet(f"{save_path}/ip_{k}_strategies")
        )


if __name__ == "__main__":
    path = sys.argv[1]
    fyear = int(sys.argv[2])

    extract_apc_data(path, fyear)
