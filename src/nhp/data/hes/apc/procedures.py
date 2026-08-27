import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.hes.apc import get_hes_apc
from nhp.data.hes.fix_icd10_or_opcs4 import fix_icd10_or_opcs4
from nhp.data.table_names import table_names


def get_hes_apc_procedures(spark: SparkSession) -> DataFrame:
    n = 24
    stack_cols = ", ".join(
        f"{i}, opertn_{i:02}, opdate_{i:02}" for i in range(1, n + 1)
    )
    expr = f"stack({n}, {stack_cols}) as(procedure_order, procedure_code, date)"

    df = (
        get_hes_apc(spark)
        .selectExpr("epikey", "fyear", "procode3", expr)
        .filter(F.col("procedure_code").isNotNull())
        .filter(F.col("procedure_code").rlike("^[A-Z]"))
    )

    return fix_icd10_or_opcs4(df, "procedure_code")


def generate_hes_apc_procedures(spark: SparkSession) -> None:
    opertn_df = get_hes_apc_procedures(spark)

    (
        opertn_df.repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_apc_procedures)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_apc_procedures(spark)
