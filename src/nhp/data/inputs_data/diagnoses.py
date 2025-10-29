"""Generate Diagnoses Dataframe"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae.diagnoses import get_ae_diagnoses
from nhp.data.inputs_data.ip.diagnoses import get_ip_diagnoses
from nhp.data.inputs_data.op.diagnoses import get_op_diagnoses
from nhp.data.table_names import table_names


def get_diagnoses(spark: SparkSession) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ae_diagnoses, get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def save_diagnoses(path: str, spark: SparkSession) -> None:
    """Save baseline data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark sesssion to use
    :type spark: SparkSession
    """
    df = get_diagnoses(spark).toPandas()
    df.to_parquet(f"{path}/diagnoses.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_diagnoses(path, spark)
