from itertools import chain

import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all


def get_provider_successors_mapping(spark: SparkContext):
    provider_successors = spark.read.table("su_data.reference.ods_trusts").collect()

    provider_successors = {
        row["org_from"]: row["org_to"] for row in provider_successors
    }
    provider_successors_mapping = F.create_map(
        [F.lit(x) for x in chain(*provider_successors.items())]
    )

    return provider_successors_mapping


def _add_provider_column(
    df: DataFrame,
    spark: SparkContext,
    procode3_col: str = "procode3",
    sitetret_col: str = "sitetret",
) -> DataFrame:
    """Add provider column

    :param df: the data frame to add the provider column to
    :type df: DataFrame
    :param spark: The spark context to use
    :type spark: SparkContext
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


DataFrame.add_provider_column = _add_provider_column
