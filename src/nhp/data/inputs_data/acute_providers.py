"""Get Acute Providers"""

from functools import cache

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


@cache
def get_acute_providers(spark: SparkSession) -> DataFrame:
    """Get Acute Providers

    get the list of acute providers

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: data frame of acute providers
    :rtype: DataFrame
    """
    acute_df = (
        spark.read.table("strategyunit.reference.ods_trusts")
        .filter(F.col("org_type").startswith("ACUTE"))
        .persist()
    )

    return acute_df


def filter_acute_providers(
    spark: SparkSession, table_name: str, org_code_col: str = "provider"
) -> DataFrame:
    """Filter a data frame to the acute providers

    :param spark: The spark context to use
    :type spark: SparkSession
    :param table_name: the name of the table to select
    :type table_name: str
    :param org_code_col: the org code column
    :type org_code_col: str
    :return: _description_
    :rtype: DataFrame
    """
    acute_df = (
        get_acute_providers(spark)
        .select(F.col("org_to").alias(org_code_col))
        .distinct()
    )

    return spark.read.table(table_name).join(acute_df, org_code_col, "semi")
