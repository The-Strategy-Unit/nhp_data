"""Generate a list of procedures that may be performed as day cases or outpatients"""

import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from scipy.stats import binomtest


def _get_procedures(spark: SparkContext, table_name: str) -> DataFrame:
    return (
        spark.read.table(f"hes.silver.{table_name}_procedures")
        .filter(F.col("procedure_order") == 1)
        .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
        .filter(~F.col("procedure_code").rlike("^X[6-9]"))
        .filter(~F.col("procedure_code").rlike("^[UYZ]"))
    )


def get_day_procedure_code_list(
    spark: SparkContext, minimum_total: int = 100, p_value: float = 0.001
) -> None:
    """_summary_

    :param spark: The spark context to use
    :type spark: SparkContext
    :param minimum_total: what is the minimum number of procedures in total that must be performed,
        defaults to 100
    :type minimum_total: int, optional
    :param p_value: what p-value to use when we run the binomial tests, defaults to 0.001
    :type p_value: float, optional
    """
    providers = (
        spark.read.table("strategyunit.reference.ods_trusts")
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
        .persist()
    )

    fyear_criteria = F.col("fyear") == 201920

    op = (
        spark.read.table("nhp.raw_data.opa")
        .join(providers, "provider", "semi")
        .filter(fyear_criteria)
        .filter(F.col("has_procedures"))
    )

    df_op = (
        _get_procedures(spark, "opa")
        .join(op, on=["fyear", "procode3", "attendkey"], how="semi")
        .groupBy("procedure_code")
        .agg(F.count("attendkey").alias("op"))
    )

    ip = (
        spark.read.table("nhp.raw_data.apc")
        .join(providers, "provider", "semi")
        .filter(fyear_criteria)
        .filter(F.col("classpat").isin("1", "2"))
        .filter(F.col("admimeth").startswith("1"))
    )

    df_ip = (
        _get_procedures(spark, "apc")
        .join(ip, on=["fyear", "procode3", "epikey"], how="inner")
        .withColumn(
            "type", F.when(F.col("classpat") == "1", F.lit("ip")).otherwise("dc")
        )
        .groupBy("procedure_code")
        .pivot("type")
        .count()
    )

    df = (
        df_ip.join(df_op, "procedure_code", how="outer")
        .fillna(0)
        .withColumn("total", F.col("dc") + F.col("ip") + F.col("op"))
        .filter(F.col("total") >= minimum_total)
        .toPandas()
    )

    df2 = (
        df.melt(
            id_vars=["procedure_code", "total"],
            value_vars=["op", "dc"],
            var_name="type",
        )
        .assign(
            pu=lambda x: x.apply(
                lambda y: binomtest(
                    y.value, y.total, p=0.5, alternative="greater"
                ).pvalue,
                axis=1,
            )
        )
        .assign(
            po=lambda x: x.apply(
                lambda y: binomtest(
                    y.value, y.total, p=0.05, alternative="greater"
                ).pvalue,
                axis=1,
            )
        )
    )

    usually_dc = set(df2.loc[(df2.pu < p_value) & (df2.type == "dc"), "procedure_code"])
    usually_op = set(df2.loc[(df2.pu < p_value) & (df2.type == "op"), "procedure_code"])

    occasionally_dc = (
        set(df2.loc[(df2.po < p_value) & (df2.type == "dc"), "procedure_code"])
        - usually_dc
        - usually_op
    )
    occasionally_op = (
        set(df2.loc[(df2.po < p_value) & (df2.type == "op"), "procedure_code"])
        - usually_dc
        - usually_op
    )

    return {
        "usually_dc": sorted(list(usually_dc)),
        "usually_op": sorted(list(usually_op)),
        "occasionally_dc": sorted(list(occasionally_dc)),
        "occasionally_op": sorted(list(occasionally_op)),
    }
