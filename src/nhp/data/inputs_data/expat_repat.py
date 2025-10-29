"""Generate Rates Dataframe"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.expat_repat import (
    get_ae_expat_data,
    get_ae_repat_local_data,
    get_ae_repat_nonlocal_data,
)
from nhp.data.inputs_data.ip.expat_repat import (
    get_ip_expat_data,
    get_ip_repat_local_data,
    get_ip_repat_nonlocal_data,
)
from nhp.data.inputs_data.op.expat_repat import (
    get_op_expat_data,
    get_op_repat_local_data,
    get_op_repat_nonlocal_data,
)
from nhp.data.table_names import table_names


def get_expat_data(spark: SparkSession) -> DataFrame:
    """Get expat data (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The expat data
    :rtype: DataFrame
    """
    fns = [get_ae_expat_data, get_ip_expat_data, get_op_expat_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def get_repat_local_data(spark: SparkSession) -> DataFrame:
    """Get repat (local) data (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The repat (local) data
    :rtype: DataFrame
    """
    fns = [get_ae_repat_local_data, get_ip_repat_local_data, get_op_repat_local_data]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def get_repat_nonlocal_data(spark: SparkSession) -> DataFrame:
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


def save_expat_repat_data(path: str, spark: SparkSession) -> None:
    """Save expat and repat data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """

    fns = {
        "expat": get_expat_data,
        "repat_local": get_repat_local_data,
        "repat_nonlocal": get_repat_nonlocal_data,
    }

    for name, fn in fns.items():
        df = fn(spark).toPandas()
        df.to_parquet(f"{path}/{name}.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_expat_repat_data(path, spark)
