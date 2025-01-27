"""Generate Age/Sex Dataframe"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip import get_ip_age_sex_data
from inputs_data.op import get_op_age_sex_data


def get_age_sex(spark: SparkContext) -> DataFrame:
    """Get age/sex (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The age/sex data
    :rtype: DataFrame
    """
    fns = [
        lambda spark: get_ip_age_sex_data(spark).drop("speldur"),
        get_op_age_sex_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_age_sex(spark: SparkContext, path: str) -> None:
    """Generate age/sex parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_age_sex(spark).toPandas().to_parquet(f"{path}/age_sex.parquet")
