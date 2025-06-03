"""Extract data for model"""

# dbutils.widgets.text("version", "dev")
# dbutils.widgets.text("fyear", "201920")


import sys

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from model_data.helpers import create_population_projections


# pylint: disable=invalid-name
def create_custom_birth_factors_R0A66(
    spark: SparkSession, birth_factors: DataFrame
) -> DataFrame:
    """Create custom birth factors file for R0A66, using principal projection

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    custom_R0A = birth_factors.filter(
        (F.col("dataset") == "R0A") & (F.col("variant") == "principal_proj")
    ).withColumn("variant", F.lit("custom_projection_R0A66"))

    return custom_R0A


# pylint: disable=invalid-name
def create_custom_birth_factors_RD8(
    spark: SparkSession, birth_factors: DataFrame
) -> DataFrame:
    """Create custom birth factors file for RD8, using principal projection

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    custom_RD8 = birth_factors.filter(
        (F.col("dataset") == "RD8") & (F.col("variant") == "principal_proj")
    ).withColumn("variant", F.lit("custom_projection_RD8"))

    return custom_RD8


def extract_birth_factors_data(spark: SparkSession, save_path: str, fyear: int) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    births = spark.read.table("nhp.population_projections.births").withColumn(
        "sex", F.lit(2)
    )

    birth_factors = create_population_projections(spark, births, 201819)

    custom_R0A = create_custom_birth_factors_R0A66(spark, birth_factors)
    custom_RD8 = create_custom_birth_factors_RD8(spark, birth_factors)

    (
        # using a fixed year of 2018/19 to match prior logic
        birth_factors.unionByName(custom_R0A)
        .unionByName(custom_RD8)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )


def main(save_path: str, fyear: int) -> None:
    """Main method

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    spark: SparkSession = DatabricksSession.builder.getOrCreate()

    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    extract_birth_factors_data(spark, save_path, fyear)


def __init__(*args):
    path = args[0]
    fyear = int(args[1])
    main(path, fyear)


if __name__ == "__main__":
    __init__(*sys.argv[1:])
