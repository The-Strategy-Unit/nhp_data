"""Generate Procedures Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.ae.procedures import get_ae_procedures
from nhp.data.inputs_data.helpers import get_spark
from nhp.data.inputs_data.ip.procedures import get_ip_procedures
from nhp.data.inputs_data.op.procedures import get_op_procedures


def get_procedures(spark: SparkSession = get_spark()) -> DataFrame:
    """Get procedures (combined)

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The procedures data
    :rtype: DataFrame
    """
    fns = [get_ae_procedures, get_ip_procedures, get_op_procedures]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


if __name__ == "__main__":
    path = sys.argv[1]

    get_procedures().toPandas().to_parquet(f"{path}/procedures.parquet")
