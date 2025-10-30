"""Get inpatients diagnoses data"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.ip import get_ip_df, get_ip_mitigators
from nhp.data.table_names import table_names


def get_ip_procedures(spark: SparkSession) -> DataFrame:
    """Get inpatients procedures

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients procedures data
    :rtype: DataFrame
    """
    procs = (
        spark.read.table(table_names.hes_apc_procedures)
        .filter(F.col("procedure_order") == 1)
        .withColumn("procedure_code", F.col("procedure_code").substr(1, 3))
    )

    procs_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_ip_mitigators(spark)

    return (
        get_ip_df(spark)
        .join(procs, ["epikey", "fyear"])
        .join(mitigators, ["fyear", "provider", "epikey"])
        .groupBy("fyear", "provider", "strategy", "procedure_code")
        .agg(F.sum("sample_rate").alias("n"))
        .withColumn("total", F.sum("n").over(procs_w))
        .withColumn("pcnt", F.col("n") / F.col("total"))
        .withColumn("rn", F.row_number().over(procs_w.orderBy(F.desc("n"))))
        .filter(F.col("rn") <= 6)
        .filter(F.col("n") >= 5)
        .filter(F.col("total") - F.col("n") >= 5)
        .orderBy("fyear", "provider", "strategy", "rn")
    )
