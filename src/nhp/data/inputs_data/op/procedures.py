"""Get outpatients diagnoses data"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.op import get_op_df, get_op_mitigators
from nhp.data.table_names import table_names


def get_op_procedures(spark: SparkSession) -> DataFrame:
    """Get outpatients procedures

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients procedures data
    :rtype: DataFrame
    """
    procs = (
        spark.read.table(table_names.hes_opa_procedures)
        .filter(F.col("procedure_order") == 1)
        .withColumn("procedure_code", F.col("procedure_code").substr(1, 3))
    )

    procs_w = Window.partitionBy("fyear", "provider", "strategy")

    mitigators = get_op_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_op_df(spark)
        .join(mitigators, ["fyear", "provider", "attendkey"])
        .join(procs, ["attendkey", "fyear"])
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
