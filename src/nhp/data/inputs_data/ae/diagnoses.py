"""Get A&E diagnoses data"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.ae import get_ae_mitigators


def get_ae_diagnoses(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get A&E diagnoses

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The A&E diagnoses data
    :rtype: DataFrame
    """
    diags_w = Window.partitionBy("fyear", geography_column, "strategy")
    return (
        get_ae_mitigators(spark)
        .filter(F.col("n") > 0)
        .filter(F.isnotnull("primary_diagnosis"))
        .filter(F.col("primary_diagnosis") != "")
        .withColumnRenamed("primary_diagnosis", "diagnosis")
        .groupBy("fyear", geography_column, "strategy", "diagnosis")
        .agg(F.sum("n").alias("n"))
        .withColumn("total", F.sum("n").over(diags_w))
        .withColumn("pcnt", F.col("n") / F.col("total"))
        .withColumn("rn", F.row_number().over(diags_w.orderBy(F.desc("n"))))
        .filter(F.col("rn") <= 6)
        .filter(F.col("n") >= 5)
        .filter(F.col("total") - F.col("n") >= 5)
        .orderBy("fyear", geography_column, "strategy", "rn")
    )
