"""Generate Rates Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.ae.rates import get_ae_rates
from nhp.data.inputs_data.helpers import get_spark
from nhp.data.inputs_data.ip.rates import (
    get_ip_activity_avoidance_rates,
    get_ip_day_procedures,
    get_ip_mean_los,
    get_ip_preop_rates,
)
from nhp.data.inputs_data.op.rates import get_op_rates


def get_rates(spark: SparkSession = get_spark()) -> DataFrame:
    """Get rates (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The rates data
    :rtype: DataFrame
    """
    fns = [
        get_ae_rates,
        get_ip_activity_avoidance_rates,
        get_ip_mean_los,
        get_ip_preop_rates,
        get_ip_day_procedures,
        get_op_rates,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def main():
    path = sys.argv[1]

    get_rates().toPandas().to_parquet(f"{path}/rates.parquet")
