"""Get outpatients diagnoses data"""

from pyspark import SparkSession
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.op import get_op_df, get_op_mitigators


def get_op_diagnoses(spark: SparkSession) -> DataFrame:
    """Get outpatients diagnoses

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients diagnoses data
    :rtype: DataFrame
    """
    diags = (
        spark.read.table("hes.silver.opa_diagnoses")
        .filter(F.col("diag_order") == 1)
        .withColumn("diagnosis", F.col("diagnosis").substr(1, 3))
    )

    diags_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_op_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_op_df(spark)
        .join(mitigators, ["attendkey"])
        .join(diags, ["attendkey", "fyear"])
        .groupBy("fyear", "provider", "strategy", "diagnosis")
        .agg(F.sum("n").alias("n"))
        .withColumn("total", F.sum("n").over(diags_w))
        .withColumn("pcnt", F.col("n") / F.col("total"))
        .withColumn("rn", F.row_number().over(diags_w.orderBy(F.desc("n"))))
        .filter(F.col("rn") <= 6)
        .filter(F.col("n") >= 5)
        .filter(F.col("total") - F.col("n") >= 5)
        .orderBy("fyear", "provider", "strategy", "rn")
    )
