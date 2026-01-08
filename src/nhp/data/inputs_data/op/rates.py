"""Get Outpatients Rates Data"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.inputs_data.direct_standardisation import directly_standardise
from nhp.data.inputs_data.op import get_op_age_sex_data


@directly_standardise
def get_op_rates(spark: SparkSession) -> DataFrame:
    """Get outpatients activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients activity avoidances rates
    :rtype: DataFrame
    """
    return get_op_age_sex_data(spark).withColumn(
        "d",
        F.when(
            F.col("strategy").startswith("followup_reduction_"),  # ty: ignore[missing-argument, invalid-argument-type]
            F.col("d") - F.col("n"),
        ).otherwise(F.col("d")),
    )
