"""Extract ECDS data for model"""

import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from model_data.helpers import get_spark


def extract_ecds_data(
    save_path: str, fyear: int, spark: SparkSession = get_spark()
) -> None:
    """Extract ECDS data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    ecds = (
        spark.read.table("ecds")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
    )

    (
        ecds.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/aae")
    )


if __name__ == "__main__":
    path = sys.argv[1]
    fyear = int(sys.argv[2])

    extract_ecds_data(path, fyear)
