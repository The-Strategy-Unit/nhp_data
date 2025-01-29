"""Generate Rates Dataframe"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ae.expat_repat import (
    get_ae_expat_data,
    get_ae_repat_local_data,
    get_ae_repat_nonlocal_data,
)
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


def get_expat_data(spark: SparkContext) -> DataFrame:
    """Get expat data (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The expat data
    :rtype: DataFrame
    """
    fns = [get_ae_expat_data, get_ip_expat_data, get_op_expat_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_expat(spark: SparkContext, path: str) -> None:
    """Generate expat parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_expat_data(spark).toPandas().to_parquet(f"{path}/expat.parquet")


def get_repat_local_data(spark: SparkContext) -> DataFrame:
    """Get repat (local) data (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The repat (local) data
    :rtype: DataFrame
    """
    fns = [get_ae_repat_local_data, get_ip_repat_local_data, get_op_repat_local_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_repat_local(spark: SparkContext, path: str) -> None:
    """Generate repat (local) parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_repat_local_data(spark).toPandas().to_parquet(f"{path}/repat_local.parquet")


def get_repat_nonlocal_data(spark: SparkContext) -> DataFrame:
    """Get repat (non-local) data (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The repat (non-local) data
    :rtype: DataFrame
    """
    fns = [
        get_ae_repat_nonlocal_data,
        get_ip_repat_nonlocal_data,
        get_op_repat_nonlocal_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_repat_nonlocal(spark: SparkContext, path: str) -> None:
    """Generate repat (non-local) parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_repat_nonlocal_data(spark).toPandas().to_parquet(
        f"{path}/repat_nonlocal.parquet"
    )
