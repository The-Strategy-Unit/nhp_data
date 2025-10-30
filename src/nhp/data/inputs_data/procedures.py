"""Generate Procedures Dataframe"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.procedures import get_ae_procedures
from nhp.data.inputs_data.ip.procedures import get_ip_procedures
from nhp.data.inputs_data.op.procedures import get_op_procedures
from nhp.data.table_names import table_names


def get_procedures(spark: SparkSession) -> DataFrame:
    """Get procedures (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The procedures data
    :rtype: DataFrame
    """
    fns = [get_ae_procedures, get_ip_procedures, get_op_procedures]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def save_procedures(path: str, spark: SparkSession) -> None:
    """Save procedures data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_procedures(spark).toPandas()
    df.to_parquet(f"{path}/procedures.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_procedures(path, spark)
