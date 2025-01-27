"""Generate Rates Dataframe"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip.rates import (
    get_ip_activity_avoidance_rates,
    get_ip_aec_rates,
    get_ip_day_procedures,
    get_ip_mean_los,
    get_ip_preop_rates,
)
from inputs_data.op.rates import get_op_rates


def get_rates(spark: SparkContext) -> DataFrame:
    """Get rates (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The rates data
    :rtype: DataFrame
    """
    fns = [
        get_ip_activity_avoidance_rates,
        get_ip_mean_los,
        get_ip_aec_rates,
        get_ip_preop_rates,
        get_ip_day_procedures,
        get_op_rates,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_rates(spark: SparkContext, path: str) -> None:
    """Generate rates parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_rates(spark).toPandas().to_parquet(f"{path}/rates.parquet")
