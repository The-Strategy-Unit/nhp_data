"""Generate Diagnoses Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.ae.diagnoses import get_ae_diagnoses
from nhp.data.inputs_data.helpers import get_spark
from nhp.data.inputs_data.ip.diagnoses import get_ip_diagnoses
from nhp.data.inputs_data.op.diagnoses import get_op_diagnoses


def get_diagnoses(spark: SparkSession) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ae_diagnoses, get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def main():
    path = sys.argv[1]

    spark = get_spark()

    get_diagnoses(spark).toPandas().to_parquet(f"{path}/diagnoses.parquet")
