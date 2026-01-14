"""Get outpatients diagnoses data"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.op import get_op_mitigators
from nhp.data.table_names import table_names


def get_op_diagnoses(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get outpatients diagnoses

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The outpatients diagnoses data
    :rtype: DataFrame
    """
    diags = (
        spark.read.table(table_names.hes_opa_diagnoses)
        .filter(F.col("diag_order") == 1)
        .withColumn("diagnosis", F.col("diagnosis").substr(1, 3))
    )

    diags_w = Window.partitionBy("fyear", geography_column, "strategy")

    return (
        get_op_mitigators(spark)
        .filter(F.col("n") > 0)
        .join(diags, ["attendkey", "fyear"])
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
