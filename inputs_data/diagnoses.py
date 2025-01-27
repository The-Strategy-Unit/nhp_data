"""Generate Diagnoses Dataframe"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip.diagnoses import get_ip_diagnoses
from inputs_data.op.diagnoses import get_op_diagnoses


def get_diagnoses(spark: SparkContext) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_diagnoses(spark: SparkContext, path: str) -> None:
    """Generate diagnoses parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_diagnoses(spark).toPandas().to_parquet(f"{path}/diagnoses.parquet")
