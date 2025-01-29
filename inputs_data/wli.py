"""Generate WLI Dataframe"""

import sys

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.helpers import get_spark
from inputs_data.ip.wli import get_ip_wli
from inputs_data.op.wli import get_op_wli


def get_wli(spark: SparkContext = get_spark()) -> DataFrame:
    """Get WLI (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The WLI data
    :rtype: DataFrame
    """
    ip = get_ip_wli(spark)
    op = get_op_wli(spark)

    return ip.join(op, ["fyear", "provider", "tretspef"], "outer")


if __name__ == "__main__":
    path = sys.argv[1]

    get_wli().toPandas().to_parquet(f"{path}/wli.parquet")
