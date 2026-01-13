"""Generate Diagnoses Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.diagnoses import get_ae_diagnoses
from nhp.data.inputs_data.ip.diagnoses import get_ip_diagnoses
from nhp.data.inputs_data.op.diagnoses import get_op_diagnoses
from nhp.data.table_names import table_names


def get_diagnoses(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ae_diagnoses, get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark, geography_column) for f in fns])


def save_diagnoses(path: str, spark: SparkSession, geography_column: str) -> None:
    """Save baseline data.

    :param path: The path to save the data to
    :type path: str
    :param geography_column: The geography column to use
    :type geography_column: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_diagnoses(spark, geography_column).toPandas()
    df.to_parquet(f"{path}/diagnoses.parquet")


def main():
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"
    spark = get_spark()
    save_diagnoses(path, spark, geography_column)
