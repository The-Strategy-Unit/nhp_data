"""Extract demographic factors data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import (
    DEMOGRAPHICS_MAX_YEAR,
    DEMOGRAPHICS_MIN_YEAR,
    create_provider_population_projections,
)
from nhp.data.table_names import table_names


def extract(
    save_path: str, fyear: int, projection_year: int, spark: SparkSession
) -> None:
    """Extract Demographic Factors data

    :param spark: the spark session to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    demographics = spark.read.table(
        table_names.population_projections_demographics
    ).filter(F.col("year").between(DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR))

    (
        create_provider_population_projections(
            spark, demographics, fyear, projection_year
        )
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/demographic_factors/fyear={fyear // 100}")
    )


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"
    fyear = int(sys.argv[2])
    projection_year = int(sys.argv[3])

    spark = get_spark()

    extract(save_path, fyear, projection_year, spark)
