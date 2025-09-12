from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403


def local_authority_successors(
    spark: SparkSession, df: DataFrame, col_name: str
) -> DataFrame:
    """Local Authority Successors

    If a local authority has been succeeded, replace it with the successor

    :param spark: the spark context
    :type spark: SparkSession
    :param df: the dataframe to add the main icb column to
    :type df: DataFrame
    :param col_name: the column to update
    :type col_name: str
    :return: the modified dataframe
    :rtype: DataFrame
    """

    lad11_to_lad23_lookup = spark.read.csv(
        "/Volumes/nhp/reference/files/lad11_to_lad23_lookup.csv", header=True
    )

    lookup = {
        r["lad11cd"]: r["lad21cd"]
        for r in lad11_to_lad23_lookup.select("lad11cd", "lad21cd").distinct().collect()
    }

    lookup_map = F.create_map([F.lit(x) for x in chain(*lookup.items())])

    return df.withColumn(
        col_name, F.coalesce(lookup_map[F.col(col_name)], F.col(col_name))
    )
