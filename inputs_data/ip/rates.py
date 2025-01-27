"""Get Inpatients Rates Data"""

import json
from functools import cache, reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.catchments import get_catchments, get_total_pop
from inputs_data.helpers import age_group
from inputs_data.ip import get_ip_age_sex_data, get_ip_mitigators


def get_ip_activity_avoidance_rates(spark: SparkContext) -> DataFrame:
    """Get inpatients activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients activity avoidances rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)
    mitigators = (
        get_ip_mitigators(spark)
        .filter(F.col("type") == "activity_avoidance")
        .select("strategy")
        .distinct()
    )
    catchments = get_catchments(spark)
    total_pop = get_total_pop(spark)

    return (
        df.join(
            mitigators,
            "strategy",
            "semi",
        )
        .join(catchments, ["fyear", "age_group", "sex", "provider"], "inner")
        .join(total_pop, ["fyear", "age_group", "sex"], "inner")
        .groupBy("fyear", "strategy", "provider")
        .agg(
            F.sum("n").alias("numerator"),
            F.sum("pop_catch").alias("denominator"),
            (
                F.sum(F.col("n") / F.col("pop_catch") * F.col("total_pop"))
                / F.sum("total_pop")
            ).alias("rate"),
        )
    )


def get_ip_mean_los(spark: SparkContext) -> DataFrame:
    """Get inpatients mean length of stay data

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients mean los rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)

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
        "virtual_wards_activity_avoidance_ari",
        "virtual_wards_activity_avoidance_heart_failure",
    ]

    return (
        df.filter(F.col("strategy").isin(mean_los_reduction_mitigators))
        .filter(F.col("n") > 0)
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("speldur").alias("numerator"), F.sum("n").alias("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
    )


def get_ip_aec_rates(spark: SparkContext) -> DataFrame:
    """Get inpatients aec rates

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients activity avoidances rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)

    return (
        df.filter(F.col("strategy").startswith("ambulatory_emergency_care_"))
        .withColumn("n", (F.col("speldur") == 0).cast("int"))
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("n").alias("numerator"), F.count("n").alias("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
    )


def get_ip_preop_rates(spark: SparkContext) -> DataFrame:
    """Get inpatients pre-op rates

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients pre-op rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)

    opertn_counts = (
        spark.read.table("su_data.nhp.apc")
        .join(age_group(spark), "age")
        .filter(F.col("admimeth").startswith("1"))
        .groupBy("fyear", "provider", "age_group", "sex")
        .agg(F.count("has_procedure").alias("d"))
    )

    return (
        df.filter(F.col("strategy").startswith("pre-op_los_"))
        .join(opertn_counts, ["fyear", "provider", "age_group", "sex"], "inner")
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("n").alias("numerator"), F.sum("d").alias("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
    )


@cache
def _get_ip_day_procedures_code_list(spark: SparkContext) -> DataFrame:
    with open(
        "/Volumes/su_data/nhp/reference_data/day_procedures.json", "r", encoding="UTF-8"
    ) as f:
        data = [
            {"strategy": f"day_procedures_{k}", "procedure_code": v}
            for k, vs in json.load(f).items()
            for v in vs
        ]
        return spark.createDataFrame(data).persist()


def _get_ip_day_procedures_op_denominator(spark: SparkContext) -> DataFrame:
    """Get inpatients day procedures (outpatients) denominator

    :param spark: The spark context to use
    :type spark: SparkContext
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
        spark.read.table("hes.silver.opa")
        .filter(F.col("apptage").isNotNull())
        .join(op_procedures, ["attendkey", "fyear", "procode3"], "inner")
        .join(
            spark.read.table("su_data.reference.provider_successors"),
            [F.col("procode3") == F.col("old_code")],
            "inner",
        )
        .join(age_group(spark), F.col("apptage") == F.col("age"))
        .groupBy(
            "fyear", F.col("new_code").alias("provider"), "strategy", "age_group", "sex"
        )
        .agg(F.count("strategy").alias("d"))
    )


def _get_ip_day_procedures_dc_denominator(spark: SparkContext) -> DataFrame:
    """Get inpatients day procedures (daycase) denominator

    :param spark: The spark context to use
    :type spark: SparkContext
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
        spark.read.table("hes.silver.apc")
        .withColumn(
            "age",
            F.when(
                (F.col("admiage") == 999) | F.col("admiage").isNull(),
                F.when(F.col("startage") > 7000, 0).otherwise(F.col("startage")),
            ).otherwise(F.col("admiage")),
        )
        .withColumn("age", F.when(F.col("age") > 90, 90).otherwise(F.col("age")))
        .filter(F.col("age").isNotNull())
        .join(dc_procedures, ["epikey", "fyear", "procode3"], "inner")
        .join(
            spark.read.table("su_data.reference.provider_successors"),
            [F.col("procode3") == F.col("old_code")],
            "inner",
        )
        .join(age_group(spark), "age")
        .groupBy(
            "fyear", F.col("new_code").alias("provider"), "strategy", "age_group", "sex"
        )
        .agg(F.count("strategy").alias("d"))
    )


def get_ip_day_procedures(spark: SparkContext) -> DataFrame:
    """Get inpatients day procedures rates

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients day procedures  rates
    :rtype: DataFrame
    """

    df = get_ip_age_sex_data(spark)
    denominator = DataFrame.union(
        _get_ip_day_procedures_dc_denominator(spark),
        _get_ip_day_procedures_op_denominator(spark),
    )

    return (
        df.filter(F.col("strategy").startswith("day_procedures_"))
        .join(denominator, ["fyear", "strategy", "provider", "age_group", "sex"])
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("n").alias("numerator"), F.sum("d").alias("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
    )
