"""Extract birth factors data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import (
    DEMOGRAPHICS_MAX_YEAR,
    DEMOGRAPHICS_MIN_YEAR,
    create_provider_population_projections,
    extract,
)
from nhp.data.table_names import table_names


@extract("birth_factors")
def extract_birth_factors(
    save_path: str, fyear: int, spark: SparkSession, projection_year: int
) -> DataFrame:
    """Extract Birth Factors data

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    :param projection_year: the year for which to project the population
    :type projection_year: int
    :param spark: the spark session to use
    :type spark: SparkSession
    """
    births = (
        spark.read.table(table_names.population_projections_births)
        .withColumn("sex", F.lit(2))
        .filter(F.col("year").between(DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR))
    )

    return create_provider_population_projections(
        spark, births, fyear, projection_year
    ).withColumn("fyear", F.lit(fyear // 100))


def main():
    data_version = sys.argv[1]
    save_path = f"{table_names.model_data_path}/{data_version}"

    fyear = int(sys.argv[2])
    projection_year = int(sys.argv[3])

    spark = get_spark()

    extract_birth_factors(save_path, fyear, spark, projection_year)
