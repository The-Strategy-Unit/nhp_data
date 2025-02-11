"""Get A&E diagnoses data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.ae import get_ae_df, get_ae_mitigators


def get_ae_diagnoses(spark: SparkContext) -> DataFrame:
    """Get A&E diagnoses

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The A&E diagnoses data
    :rtype: DataFrame
    """
    diags_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_ae_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_ae_df(spark)
        .join(mitigators, ["fyear", "key"])
        .filter(F.col("primary_diagnosis").isNotNull())
        .filter(F.col("primary_diagnosis") != "")
        .withColumnRenamed("primary_diagnosis", "diagnosis")
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
