"""Generate Age/Sex Dataframe"""

import sys
from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from inputs_data.ae import get_ae_age_sex_data
from inputs_data.helpers import get_spark
from inputs_data.ip import get_ip_age_sex_data
from inputs_data.op import get_op_age_sex_data


def get_age_sex(spark: SparkContext = get_spark()) -> DataFrame:
    """Get age/sex (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The age/sex data
    :rtype: DataFrame
    """
    fns = [
        get_ae_age_sex_data,
        lambda spark: get_ip_age_sex_data(spark).drop("speldur"),
        get_op_age_sex_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_age_sex().filter(F.col("n") > 5).toPandas().to_parquet(
        f"{path}/age_sex.parquet"
    )
