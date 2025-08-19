"""Generate WLI Dataframe"""

import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from inputs_data.helpers import get_spark
from inputs_data.ip.wli import get_ip_wli
from inputs_data.op.wli import get_op_wli
from nhp_datasets.providers import get_provider_successors_mapping


def get_wli(path: str, spark: SparkSession = get_spark()) -> DataFrame:
    """Get WLI (combined)

    :param path: Where to read the waiting list avg change file from
    :type path: str
    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The WLI data
    :rtype: DataFrame
    """
    ip = get_ip_wli(spark)
    op = get_op_wli(spark)

    w = Window.partitionBy("provider", "tretspef").orderBy("date")

    provider_successors_mapping = get_provider_successors_mapping(spark)

    # TODO: this table is generated using a query on our Data Warehouse, but using publically available RTT files
    # ideally this should be made more reproducible
    wl_ac = (
        spark.read.parquet(f"{path}/rtt_year_end_incompletes.parquet")
        .withColumn("provider", provider_successors_mapping[F.col("procode3")])
        .groupBy("provider", "tretspef", "date")
        .agg(F.sum("n").alias("n"))
        .withColumn("diff", F.col("n") - F.lag("n", 1).over(w))
        .withColumn(
            "avg_change",
            F.avg("diff").over(
                w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn("fyear", (F.year("date") - 1) * 100 + (F.year("date") % 100))
        .filter(F.col("fyear") >= 201920)
        .drop("date", "n", "diff")
    )

    return (
        wl_ac.filter(F.col("avg_change") != 0)
        .join(ip, ["fyear", "provider", "tretspef"], "left")
        .join(op, ["fyear", "provider", "tretspef"], "left")
        .fillna(0)
    )


if __name__ == "__main__":
    path = sys.argv[1]

    get_wli(path).toPandas().to_parquet(f"{path}/wli.parquet")
