"""Generate WLI Dataframe"""

from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip.wli import get_ip_wli
from inputs_data.op.wli import get_op_wli


def get_wli(spark: SparkContext) -> DataFrame:
    """Get WLI (combined)

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The WLI data
    :rtype: DataFrame
    """
    ip = get_ip_wli(spark)
    op = get_op_wli(spark)

    return ip.join(op, ["fyear", "provider", "tretspef"], "outer")


def generate_wli(spark: SparkContext, path: str) -> None:
    """Generate WLI parquet file

    :param spark: The spark context to use
    :type spark: SparkContext
    :param path: Where to save the paruqet file
    :type path: str
    """
    get_wli(spark).toPandas().to_parquet(f"{path}/wli.parquet")
