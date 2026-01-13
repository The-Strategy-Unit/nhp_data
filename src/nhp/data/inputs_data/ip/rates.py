"""Get Inpatients Rates Data"""

import json
from functools import cache

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.direct_standardisation import directly_standardise
from nhp.data.inputs_data.ip import get_ip_age_sex_data
from nhp.data.reference.population_by_lsoa21 import get_pop_by_lad23
from nhp.data.reference.provider_catchments import get_pop_by_provider
from nhp.data.table_names import table_names


def get_population(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get population data for inpatients rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The population data
    :rtype: DataFrame
    """

    match geography_column:
        case "lad23cd":
            df = get_pop_by_lad23(spark)
        case "provider":
            df = get_pop_by_provider(spark)
        case _:
            raise ValueError(f"Unsupported geography column: {geography_column}")

    return df.withColumnRenamed("population", "d")


@directly_standardise
def get_ip_activity_avoidance_rates(
    spark: SparkSession, geography_column: str
) -> DataFrame:
    """Get inpatients activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients activity avoidances rates
    :rtype: DataFrame
    """

    pop = get_population(spark, geography_column)

    return (
        get_ip_age_sex_data(spark, geography_column)
        .filter(
            (F.col("type") == "activity_avoidance")
            # sdec is technically an efficiency mitigator, but behaves like an
            # activity avoidance mitigator
            | F.col("strategy").startswith("same_day_emergency_care_")  # ty: ignore[missing-argument, invalid-argument-type]
        )
        .join(pop, ["fyear", "age", "sex", geography_column], "inner")
        .drop("speldur")
    )


@directly_standardise
def get_ip_mean_los(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get inpatients mean length of stay data

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients mean los rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark, geography_column)

    mean_los_reduction_mitigators = [
        "emergency_elderly",
        "enhanced_recovery_bladder",
        "enhanced_recovery_breast",
        "enhanced_recovery_colectomy",
        "enhanced_recovery_hip",
        "enhanced_recovery_hysterectomy",
        "enhanced_recovery_knee",
        "enhanced_recovery_prostate",
        "enhanced_recovery_rectum",
        "excess_beddays_elective",
        "excess_beddays_emergency",
        "general_los_reduction_elective",
        "general_los_reduction_emergency",
        "raid_ip",
        "stroke_early_supported_discharge",
        "virtual_wards_efficiencies_ari",
        "virtual_wards_efficiencies_heart_failure",
    ]

    return (
        df.filter(F.col("strategy").isin(mean_los_reduction_mitigators))
        .withColumnRenamed("n", "d")
        .withColumnRenamed("speldur", "n")
    )


@directly_standardise
def get_ip_preop_rates(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get inpatients pre-op rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients pre-op rates
    :rtype: DataFrame
    """

    opertn_counts = (
        spark.read.table(table_names.raw_data_apc)
        .filter(F.col("admimeth").startswith("1"))  # ty: ignore[missing-argument, invalid-argument-type]
        .groupBy("fyear", geography_column)
        .agg(F.count("has_procedure").alias("d"))
    )

    return (
        get_ip_age_sex_data(spark, geography_column)
        .filter(F.col("strategy").startswith("pre-op_los_"))  # ty: ignore[missing-argument, invalid-argument-type]
        .join(opertn_counts, ["fyear", geography_column], "inner")
        .drop("speldur")
    )


@cache
def _get_ip_day_procedures_code_list(spark: SparkSession) -> DataFrame:
    with open(
        table_names.reference_day_procedures_code_list, "r", encoding="UTF-8"
    ) as f:
        data = [
            {"strategy": f"day_procedures_{k}", "procedure_code": v}
            for k, vs in json.load(f).items()
            for v in vs
        ]
        return spark.createDataFrame(data).persist()


def _get_ip_day_procedures_op_denominator(
    spark: SparkSession, geography_column: str
) -> DataFrame:
    """Get inpatients day procedures (outpatients) denominator

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients day procedures (outpatients) denominator
    :rtype: DataFrame
    """

    day_procedures = _get_ip_day_procedures_code_list(spark).filter(
        F.col("strategy").endswith("op")  # ty: ignore[missing-argument, invalid-argument-type]
    )

    op_procedures = (
        spark.read.table(table_names.hes_opa_procedures)
        .filter(F.col("procedure_order") == 1)
        .join(day_procedures, ["procedure_code"], "inner")
    )

    return (
        filter_acute_providers(spark, table_names.raw_data_opa)
        .join(op_procedures, ["fyear", "attendkey"], "inner")
        .groupBy("fyear", geography_column, "strategy", "age", "sex")
        .agg(F.count("strategy").alias("d"))
    )


def _get_ip_day_procedures_dc_denominator(
    spark: SparkSession, geography_column: str
) -> DataFrame:
    """Get inpatients day procedures (daycase) denominator

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients day procedures (daycase) denominator
    :rtype: DataFrame
    """

    day_procedures = _get_ip_day_procedures_code_list(spark).filter(
        F.col("strategy").endswith("dc")  # ty: ignore[missing-argument, invalid-argument-type]
    )

    dc_procedures = (
        spark.read.table(table_names.hes_apc_procedures)
        .filter(F.col("procedure_order") == 1)
        .join(day_procedures, ["procedure_code"], "inner")
    )

    return (
        filter_acute_providers(spark, table_names.raw_data_apc)
        .join(dc_procedures, ["fyear", "epikey"], "inner")
        .groupBy("fyear", geography_column, "strategy", "age", "sex")
        .agg(F.count("strategy").alias("d"))
    )


@directly_standardise
def get_ip_day_procedures(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get inpatients day procedures rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients day procedures  rates
    :rtype: DataFrame
    """

    denominator = DataFrame.union(
        _get_ip_day_procedures_dc_denominator(spark, geography_column),
        _get_ip_day_procedures_op_denominator(spark, geography_column),
    )

    return (
        get_ip_age_sex_data(spark, geography_column)
        .join(denominator, ["fyear", "strategy", geography_column, "age", "sex"])
        .withColumn("d", F.col("n") + F.col("d"))
    )
