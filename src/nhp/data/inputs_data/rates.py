"""Generate Rates Dataframe"""

import sys
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


def get_rates(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get rates (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
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

    return reduce(DataFrame.unionByName, [f(spark, geography_column) for f in fns])


def save_rates(path: str, spark: SparkSession, geography_column: str) -> None:
    """Save rates data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark session to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    """
    df = get_rates(spark, geography_column).toPandas()
    df.to_parquet(f"{path}/rates.parquet")


def main():
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"
    spark = get_spark()
    save_rates(path, spark, geography_column)
