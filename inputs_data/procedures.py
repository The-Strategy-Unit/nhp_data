"""Generate Procedures Dataframe"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip.procedures import get_ip_procedures
from inputs_data.op.procedures import get_op_procedures


def get_procedures(spark: SparkContext) -> DataFrame:
    """Get procedures (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The procedures data
    :rtype: DataFrame
    """
    fns = [get_ip_procedures, get_op_procedures]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def generate_procedures(spark: SparkContext, path: str) -> None:
    """Generate procedures parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_procedures(spark).toPandas().to_parquet(f"{path}/procedures.parquet")
