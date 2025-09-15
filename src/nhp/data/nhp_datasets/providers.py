from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *  # noqa: F403


def get_provider_successors_mapping(spark: SparkSession):
    provider_successors = spark.read.table("nhp.reference.ods_trusts").collect()

    provider_successors = {
        row["org_from"]: row["org_to"] for row in provider_successors
    }
    provider_successors_mapping = F.create_map(
        [F.lit(x) for x in chain(*provider_successors.items())]
    )

    return provider_successors_mapping


def add_provider(
    spark: SparkSession,
    df: DataFrame,
    procode3_col: str = "procode3",
    sitetret_col: str = "sitetret",
) -> DataFrame:
    """Add the provider column

    :param spark: The spark context to use
    :type spark: SparkSession
    :param df: The data frame
    :type df: DataFrame
    :param procode3_col: the column which contains the procode3 field, defaults to "procode3"
    :type procode3_col: str, optional
    :param sitetret_col: the column which contains the sitetret field, defaults to "sitetret"
    :type sitetret_col: str, optional
    :return: the data frame with provider column added
    :rtype: DataFrame
    """
    provider_successors_mapping = get_provider_successors_mapping(spark)

    return df.withColumn(
        "provider",
        F.when(F.col(sitetret_col) == "RW602", "R0A")
        .when(F.col(sitetret_col) == "RM318", "R0A")
        .otherwise(provider_successors_mapping[F.col(procode3_col)]),
    )


def read_data_with_provider(
    spark: SparkSession,
    table: str,
    procode3_col: str = "procode3",
    sitetret_col: str = "sitetret",
) -> DataFrame:
    """Read a table and add the provider column

    :param spark: The spark context to use
    :type spark: SparkSession
    :param table: The table to load
    :type table: str
    :param procode3_col: the column which contains the procode3 field, defaults to "procode3"
    :type procode3_col: str, optional
    :param sitetret_col: the column which contains the sitetret field, defaults to "sitetret"
    :type sitetret_col: str, optional
    :return: the data frame with provider column added
    :rtype: DataFrame
    """

    df = spark.read.table(table)
    return add_provider(spark, df, procode3_col, sitetret_col)
