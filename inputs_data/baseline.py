"""Baseline Data"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from inputs_data.ae.baseline import get_ae_baseline
from inputs_data.helpers import get_spark
from inputs_data.ip.baseline import get_ip_baseline
from inputs_data.op.baseline import get_op_baseline


def get_baseline(spark: SparkSession = get_spark()) -> DataFrame:
    """Get Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The baseline data
    :rtype: DataFrame
    """
    fns = [get_ae_baseline, get_ip_baseline, get_op_baseline]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_baseline().toPandas().to_parquet(f"{path}/baseline.parquet")
