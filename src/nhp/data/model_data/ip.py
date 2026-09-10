"""Extract IP data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import extract
from nhp.data.table_names import table_names

APC_EXCLUDE_COLS = {
    "person_id",
    "admiage",
    "imd_decile",
    "imd_quintile",
    "lsoa11",
    "lad23cd",
    "operstat",
    "icb",
    "primary_procedure",
}


@extract("ip", True, APC_EXCLUDE_COLS)
def extract_ip(save_path: str, fyear: int, spark: SparkSession) -> DataFrame:
    """Extract Inpatients data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    return (
        spark.read.table(table_names.default_apc)
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("epikey", "rn")
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("sex", F.col("sex").cast("int"))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
        .fillna({"sitetret": "unknown"})
    )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])

    spark = get_spark()

    extract_ip(save_path, fyear, spark)
