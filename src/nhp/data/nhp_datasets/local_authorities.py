from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.table_names import table_names


def lsoa11_to_lad23(spark: SparkSession, df: DataFrame, col_name: str) -> DataFrame:
    """Get the LAD23 code for a given LSOA11 code column

    :param spark: the spark context to use
    :type spark: SparkSession
    :param df: the dataframe to add the lad23cd column to
    :type df: DataFrame
    :param col_name: the column to update
    :type col_name: str
    :return: the modified dataframe
    :rtype: DataFrame
    """
    lad23_lookup = spark.read.table(
        table_names.reference_lsoa11_to_lad23,
    ).withColumnRenamed("lsoa11cd", col_name)

    return df.join(lad23_lookup, col_name, "left")
