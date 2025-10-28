"""Baseline Data"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.baseline import get_ae_baseline
from nhp.data.inputs_data.ip.baseline import get_ip_baseline
from nhp.data.inputs_data.op.baseline import get_op_baseline


def get_baseline(spark: SparkSession) -> DataFrame:
    """Get Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The baseline data
    :rtype: DataFrame
    """
    fns = [get_ae_baseline, get_ip_baseline, get_op_baseline]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def main():
    path = sys.argv[1]

    spark = get_spark()

    get_baseline(spark).toPandas().to_parquet(f"{path}/baseline.parquet")
