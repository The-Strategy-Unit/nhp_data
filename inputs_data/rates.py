"""Generate Rates Dataframe"""

import sys
from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ae.rates import get_ae_rates
from inputs_data.helpers import get_spark
from inputs_data.ip.rates import (
    get_ip_activity_avoidance_rates,
    get_ip_day_procedures,
    get_ip_mean_los,
    get_ip_preop_rates,
)
from inputs_data.op.rates import get_op_rates


def get_rates(spark: SparkContext = get_spark()) -> DataFrame:
    """Get rates (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
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


if __name__ == "__main__":
    path = sys.argv[1]

    get_rates().toPandas().to_parquet(f"{path}/rates.parquet")
