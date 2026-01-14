"""Get A&E Rates Data"""

from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.ae import get_ae_age_sex_data
from nhp.data.inputs_data.direct_standardisation import directly_standardise


@directly_standardise
def get_ae_rates(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get A&E activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The A&E activity avoidances rates
    :rtype: DataFrame
    """
    return get_ae_age_sex_data(spark, geography_column)
