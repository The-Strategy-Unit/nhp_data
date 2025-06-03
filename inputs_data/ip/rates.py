"""Get Inpatients Rates Data"""

import json
from functools import cache

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from inputs_data.acute_providers import filter_acute_providers
from inputs_data.catchments import get_catchments, get_total_pop
from inputs_data.helpers import complete_age_sex_data
from inputs_data.ip import get_ip_age_sex_data, get_ip_df, get_ip_mitigators


def get_ip_activity_avoidance_rates(spark: SparkSession) -> DataFrame:
    """Get inpatients activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients activity avoidances rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)
    df = complete_age_sex_data(spark, df, "full")

    mitigators = (
        get_ip_mitigators(spark)
        .filter(
            (F.col("type") == "activity_avoidance")
            # sdec is technically an efficiency mitigator, but behaves like an
            # activity avoidance mitigator
            | F.col("strategy").startswith("same_day_emergency_care_")
        )
        .select("strategy")
        .distinct()
    )
    catchments = get_catchments(spark)
    total_pop = get_total_pop(spark)

    df = (
        df.join(
            mitigators,
            "strategy",
            "semi",
        )
        .join(catchments, ["fyear", "age_group", "sex", "provider"], "inner")
        .join(total_pop, ["fyear", "age_group", "sex"], "inner")
    )

    df_provider = df.groupBy("fyear", "strategy", "provider").agg(
        F.sum("n").alias("numerator"),
        F.sum("pop_catch").alias("denominator"),
        (
            F.sum(F.col("n") / F.col("pop_catch") * F.col("total_pop"))
            / F.sum("total_pop")
            * 1000
        ).alias("rate"),
    )

    df_national = df.groupBy("fyear", "strategy").agg(
        (
            F.sum(F.col("n") / F.col("pop_catch") * F.col("total_pop"))
            / F.sum("total_pop")
            * 1000
        ).alias("national_rate"),
    )

    return df_provider.join(df_national, ["fyear", "strategy"])


def get_ip_mean_los(spark: SparkSession) -> DataFrame:
    """Get inpatients mean length of stay data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients mean los rates
    :rtype: DataFrame
    """

    df = get_ip_df(spark)
    df_mitigators = get_ip_mitigators(spark)

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

    w = Window.partitionBy("fyear", "strategy")

    return (
        df.join(df_mitigators, "epikey", "inner")
        .filter(F.col("strategy").isin(mean_los_reduction_mitigators))
        .groupBy("fyear", "strategy", "provider")
        .agg(
            F.sum("speldur").alias("numerator"), F.count("speldur").alias("denominator")
        )
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
        .withColumn(
            "national_rate", F.sum("numerator").over(w) / F.sum("denominator").over(w)
        )
    )


def get_ip_preop_rates(spark: SparkSession) -> DataFrame:
    """Get inpatients pre-op rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients pre-op rates
    :rtype: DataFrame
    """

    df = get_ip_df(spark)
    df_mitigators = get_ip_mitigators(spark)

    opertn_counts = (
        spark.read.table("nhp.raw_data.apc")
        .filter(F.col("admimeth").startswith("1"))
        .groupBy("fyear", "provider")
        .agg(F.count("has_procedure").alias("denominator"))
    )

    w = Window.partitionBy("fyear", "strategy")

    return (
        df.join(df_mitigators, "epikey", "inner")
        .filter(F.col("strategy").startswith("pre-op_los_"))
        .groupBy("fyear", "strategy", "provider")
        .agg(F.count("strategy").alias("numerator"))
        .join(opertn_counts, ["fyear", "provider"], "inner")
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
        .withColumn(
            "national_rate", F.sum("numerator").over(w) / F.sum("denominator").over(w)
        )
    )


@cache
def _get_ip_day_procedures_code_list(spark: SparkSession) -> DataFrame:
    with open(
        "/Volumes/nhp/reference/files/day_procedures.json", "r", encoding="UTF-8"
    ) as f:
        data = [
            {"strategy": f"day_procedures_{k}", "procedure_code": v}
            for k, vs in json.load(f).items()
            for v in vs
        ]
        return spark.createDataFrame(data).persist()


def _get_ip_day_procedures_op_denominator(spark: SparkSession) -> DataFrame:
    """Get inpatients day procedures (outpatients) denominator

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients day procedures (outpatients) denominator
    :rtype: DataFrame
    """

    day_procedures = _get_ip_day_procedures_code_list(spark).filter(
        F.col("strategy").endswith("op")
    )

    op_procedures = (
        spark.read.table("hes.silver.opa_procedures")
        .filter(F.col("procedure_order") == 1)
        .join(day_procedures, ["procedure_code"], "inner")
    )

    return (
        filter_acute_providers(spark, "opa")
        .join(op_procedures, ["fyear", "attendkey"], "inner")
        .groupBy("fyear", "provider", "strategy")
        .agg(F.count("strategy").alias("denominator"))
    )


def _get_ip_day_procedures_dc_denominator(spark: SparkSession) -> DataFrame:
    """Get inpatients day procedures (daycase) denominator

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients day procedures (daycase) denominator
    :rtype: DataFrame
    """

    day_procedures = _get_ip_day_procedures_code_list(spark).filter(
        F.col("strategy").endswith("dc")
    )

    dc_procedures = (
        spark.read.table("hes.silver.apc_procedures")
        .filter(F.col("procedure_order") == 1)
        .join(day_procedures, ["procedure_code"], "inner")
    )

    return (
        filter_acute_providers(spark, "apc")
        .join(dc_procedures, ["fyear", "epikey"], "inner")
        .groupBy("fyear", "provider", "strategy")
        .agg(F.count("strategy").alias("denominator"))
    )


def get_ip_day_procedures(spark: SparkSession) -> DataFrame:
    """Get inpatients day procedures rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients day procedures  rates
    :rtype: DataFrame
    """

    df = get_ip_df(spark)
    df_mitigators = get_ip_mitigators(spark)

    denominator = DataFrame.union(
        _get_ip_day_procedures_dc_denominator(spark),
        _get_ip_day_procedures_op_denominator(spark),
    )

    w = Window.partitionBy("fyear", "strategy")

    return (
        df.join(df_mitigators, "epikey", "inner")
        .groupBy("fyear", "strategy", "provider")
        .agg(F.count("strategy").alias("numerator"))
        .join(denominator, ["fyear", "strategy", "provider"])
        .withColumn("denominator", F.col("numerator") + F.col("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
        .withColumn(
            "national_rate", F.sum("numerator").over(w) / F.sum("denominator").over(w)
        )
    )
