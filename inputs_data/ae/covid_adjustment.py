"""A&E Covid Adjustment Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from inputs_data.ae import get_ae_df


def get_ae_covid_adjustment(spark: SparkSession) -> DataFrame:
    """Get A&E Covid Adjustment Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E covid adjustment data
    :rtype: DataFrame
    """
    return (
        get_ae_df(spark)
        .filter(F.col("fyear").between(201617, 201920))
        .withColumn("month", F.month("arrival_date"))
        .groupBy("fyear", "provider", "group", "month")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("aae"))
    )
