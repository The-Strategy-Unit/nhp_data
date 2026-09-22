"""Helper methods/tables"""

from pyspark.sql import SparkSession

from nhp.data.inputs_data.save_parquet import save_parquet
from nhp.data.table_names import table_names


def extract_demographic_factors(spark: SparkSession) -> None:
    """Extract Demographic Factors data

    :param spark: the spark session to use
    :type spark: SparkSession
    """
    df = spark.read.table(table_names.reference_population_provider_demographics)
    save_parquet(df, "demographic_factors")


def extract_birth_factors(spark: SparkSession) -> None:
    """Extract Birth Factors data

    :param spark: the spark session to use
    :type spark: SparkSession
    """
    df = spark.read.table(table_names.reference_population_provider_births)
    save_parquet(df, "birth_factors")
