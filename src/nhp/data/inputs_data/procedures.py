"""Generate Procedures Dataframe"""

import sys
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.ae.procedures import get_ae_procedures
from nhp.data.inputs_data.ip.procedures import get_ip_procedures
from nhp.data.inputs_data.op.procedures import get_op_procedures
from nhp.data.table_names import table_names


def get_procedures(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get procedures (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The procedures data
    :rtype: DataFrame
    """
    fns = [get_ae_procedures, get_ip_procedures, get_op_procedures]

    return reduce(DataFrame.unionByName, [f(spark, geography_column) for f in fns])


def save_procedures(path: str, spark: SparkSession, geography_column: str) -> None:
    """Save procedures data.

    :param path: The path to save the data to
    :type path: str
    :param geography_column: The geography column to use
    :type geography_column: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_procedures(spark, geography_column)

    if geography_column == "provider":
        df = filter_acute_providers(spark, df, "provider")
    df = df.filter(F.col(geography_column) != "unknown")

    df.toPandas().to_parquet(f"{path}/procedures.parquet")


def main():
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"
    spark = get_spark()
    save_procedures(path, spark, geography_column)
