"""Generate Diagnoses Dataframe"""

import sys
from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ae.diagnoses import get_ae_diagnoses
from inputs_data.helpers import get_spark
from inputs_data.ip.diagnoses import get_ip_diagnoses
from inputs_data.op.diagnoses import get_op_diagnoses


def get_diagnoses(spark: SparkContext = get_spark()) -> DataFrame:
    """Get Diagnoses (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The diagnoses data
    :rtype: DataFrame
    """
    fns = [get_ae_diagnoses, get_ip_diagnoses, get_op_diagnoses]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_diagnoses().toPandas().to_parquet(f"{path}/diagnoses.parquet")
