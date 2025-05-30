"""Generate Rates Dataframe"""

import sys
from functools import reduce

from pyspark import SparkSession
from pyspark.sql import DataFrame

from inputs_data.ae.expat_repat import (
    get_ae_expat_data,
    get_ae_repat_local_data,
    get_ae_repat_nonlocal_data,
)
from inputs_data.helpers import get_spark
from inputs_data.ip.expat_repat import (
    get_ip_expat_data,
    get_ip_repat_local_data,
    get_ip_repat_nonlocal_data,
)
from inputs_data.op.expat_repat import (
    get_op_expat_data,
    get_op_repat_local_data,
    get_op_repat_nonlocal_data,
)


def get_expat_data(spark: SparkSession = get_spark()) -> DataFrame:
    """Get expat data (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The expat data
    :rtype: DataFrame
    """
    fns = [get_ae_expat_data, get_ip_expat_data, get_op_expat_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def get_repat_local_data(spark: SparkSession = get_spark()) -> DataFrame:
    """Get repat (local) data (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The repat (local) data
    :rtype: DataFrame
    """
    fns = [get_ae_repat_local_data, get_ip_repat_local_data, get_op_repat_local_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def get_repat_nonlocal_data(spark: SparkSession = get_spark()) -> DataFrame:
    """Get repat (non-local) data (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The repat (non-local) data
    :rtype: DataFrame
    """
    fns = [
        get_ae_repat_nonlocal_data,
        get_ip_repat_nonlocal_data,
        get_op_repat_nonlocal_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_expat_data().toPandas().to_parquet(f"{path}/expat.parquet")
    get_repat_local_data().toPandas().to_parquet(f"{path}/repat_local.parquet")
    get_repat_nonlocal_data().toPandas().to_parquet(f"{path}/repat_nonlocal.parquet")
