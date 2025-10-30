"""Generate Rates Dataframe"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.rates import get_ae_rates
from nhp.data.inputs_data.ip.rates import (
    get_ip_activity_avoidance_rates,
    get_ip_day_procedures,
    get_ip_mean_los,
    get_ip_preop_rates,
)
from nhp.data.inputs_data.op.rates import get_op_rates
from nhp.data.table_names import table_names


def get_rates(spark: SparkSession) -> DataFrame:
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


def save_rates(path: str, spark: SparkSession) -> None:
    """Save rates data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_rates(spark).toPandas()
    df.to_parquet(f"{path}/rates.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_rates(path, spark)
