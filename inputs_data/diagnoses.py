"""Generate Diagnoses Dataframe"""

# when running on databricks, we might need to change directory in order to
# import things correctly
import os

if not os.path.exists("readme.md"):
    os.chdir("..")

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from inputs_data.ae.diagnoses import get_ae_diagnoses
from inputs_data.helpers import get_spark
from inputs_data.ip.diagnoses import get_ip_diagnoses
from inputs_data.op.diagnoses import get_op_diagnoses


def get_diagnoses(spark: SparkSession = get_spark()) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ae_diagnoses, get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_diagnoses().toPandas().to_parquet(f"{path}/diagnoses.parquet")
