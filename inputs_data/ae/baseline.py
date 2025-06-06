"""A&E Baseline Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from inputs_data.ae import get_ae_df


def get_ae_baseline(spark: SparkSession) -> DataFrame:
    """Get A&E Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E baseline data
    :rtype: DataFrame
    """
    return (
        get_ae_df(spark)
        .groupBy("fyear", "provider", "group")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("aae"))
        .withColumn("tretspef", F.lit(None).cast("string"))
    )
