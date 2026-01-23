"""Baseline Data"""

import sys
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.ae.baseline import get_ae_baseline
from nhp.data.inputs_data.ip.baseline import get_ip_baseline
from nhp.data.inputs_data.op.baseline import get_op_baseline
from nhp.data.table_names import table_names


def get_baseline(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The baseline data
    :rtype: DataFrame
    """
    fns = [get_ae_baseline, get_ip_baseline, get_op_baseline]

    return reduce(DataFrame.unionByName, [f(spark, geography_column) for f in fns])


def save_baseline(path: str, spark: SparkSession, geography_column: str) -> None:
    """Save baseline data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_baseline(spark, geography_column)

    if geography_column == "provider":
        df = filter_acute_providers(spark, df, "provider")
    df = df.filter(F.col(geography_column) != "unknown")

    df.toPandas().to_parquet(f"{path}/baseline.parquet")


def main():
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"
    spark = get_spark()
    save_baseline(path, spark, geography_column)
