"""Generate Age/Sex Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.inputs_data.ae import get_ae_age_sex_data
from nhp.data.inputs_data.helpers import get_spark
from nhp.data.inputs_data.ip import get_ip_age_sex_data
from nhp.data.inputs_data.op import get_op_age_sex_data


def get_age_sex(spark: SparkSession) -> DataFrame:
    """Get age/sex (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The age/sex data
    :rtype: DataFrame
    """
    fns = [
        get_ae_age_sex_data,
        lambda spark: get_ip_age_sex_data(spark).drop("speldur"),
        get_op_age_sex_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def main():
    path = sys.argv[1]

    spark = get_spark()

    get_age_sex(spark).filter(F.col("n") > 5).toPandas().to_parquet(
        f"{path}/age_sex.parquet"
    )
