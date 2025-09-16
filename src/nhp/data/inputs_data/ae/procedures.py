"""Get A&E diagnoses data"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.ae import get_ae_df, get_ae_mitigators


def get_ae_procedures(spark: SparkSession) -> DataFrame:
    """Get A&E procedures

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E procedures data
    :rtype: DataFrame
    """

    procs_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_ae_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_ae_df(spark)
        .join(mitigators, ["fyear", "key"])
        .filter(F.isnotnull("primary_treatment"))
        .filter(F.col("primary_treatment") != "")
        .withColumnRenamed("primary_treatment", "procedure_code")
        .groupBy("fyear", "provider", "strategy", "procedure_code")
        .agg(F.count("n").alias("n"))
        .withColumn("total", F.sum("n").over(procs_w))
        .withColumn("pcnt", F.col("n") / F.col("total"))
        .withColumn("rn", F.row_number().over(procs_w.orderBy(F.desc("n"))))
        .filter(F.col("rn") <= 6)
        .filter(F.col("n") >= 5)
        .filter(F.col("total") - F.col("n") >= 5)
        .orderBy("fyear", "provider", "strategy", "rn")
    )
