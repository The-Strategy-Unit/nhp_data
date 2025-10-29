"""Baseline Data"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.baseline import get_ae_baseline
from nhp.data.inputs_data.ip.baseline import get_ip_baseline
from nhp.data.inputs_data.op.baseline import get_op_baseline
from nhp.data.table_names import table_names


def get_baseline(spark: SparkSession) -> DataFrame:
    """Get Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The baseline data
    :rtype: DataFrame
    """
    fns = [get_ae_baseline, get_ip_baseline, get_op_baseline]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def save_baseline(path: str, spark: SparkSession) -> None:
    """Save baseline data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark sesssion to use
    :type spark: SparkSession
    """
    df = get_baseline(spark).toPandas()
    df.to_parquet(f"{path}/baseline.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_baseline(path, spark)
