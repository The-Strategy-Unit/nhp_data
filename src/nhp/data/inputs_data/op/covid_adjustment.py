"""Outpatients Covid Adjustment Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.op import get_op_df


def get_op_covid_adjustment(spark: SparkSession) -> DataFrame:
    """Get Outpatients Covid Adjustment Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The Outpatients covid adjustment data
    :rtype: DataFrame
    """
    op_attendance_month = spark.read.table("hes.silver.opa").select(
        F.col("attendkey"), F.month("apptdate").alias("month")
    )

    return (
        get_op_df(spark)
        .filter(F.col("fyear").between(201617, 201920))
        .join(op_attendance_month, "attendkey")
        .groupBy("fyear", "provider", "group", "month")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("op"))
    )
