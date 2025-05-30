"""Get inpatients diagnoses data"""

from pyspark import SparkSession
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.ip import get_ip_df, get_ip_mitigators


def get_ip_diagnoses(spark: SparkSession) -> DataFrame:
    """Get inpatients diagnoses

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients diagnoses data
    :rtype: DataFrame
    """
    diags = (
        spark.read.table("hes.silver.apc_diagnoses")
        .filter(F.col("diag_order") == 1)
        .withColumn("diagnosis", F.col("diagnosis").substr(1, 3))
    )

    diags_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_ip_mitigators(spark)

    return (
        get_ip_df(spark)
        .join(diags, ["epikey", "fyear"])
        .join(mitigators, ["epikey"])
        .groupBy("fyear", "provider", "strategy", "diagnosis")
        .agg(F.sum("sample_rate").alias("n"))
        .withColumn("total", F.sum("n").over(diags_w))
        .withColumn("pcnt", F.col("n") / F.col("total"))
        .withColumn("rn", F.row_number().over(diags_w.orderBy(F.desc("n"))))
        .filter(F.col("rn") <= 6)
        .filter(F.col("n") >= 5)
        .filter(F.col("total") - F.col("n") >= 5)
        .orderBy("fyear", "provider", "strategy", "rn")
    )
