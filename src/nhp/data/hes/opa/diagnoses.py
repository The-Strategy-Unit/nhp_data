import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.hes.fix_icd10_or_opcs4 import fix_icd10_or_opcs4
from nhp.data.hes.opa import get_hes_opa
from nhp.data.table_names import table_names


def get_hes_opa_diagnoses(spark: SparkSession) -> DataFrame:
    to_melt = [f"diag_{i:02}" for i in range(1, 13)]
    melt_str = ",".join([f"{i + 1}, `{c}`" for (i, c) in enumerate(to_melt)])

    stack_expr = F.expr(f"stack({len(to_melt)}, {melt_str}) as (diag_order, diagnosis)")

    diag_df = (
        get_hes_opa(spark)
        .select("attendkey", "fyear", "procode3", stack_expr)
        .filter(F.col("diagnosis").isNotNull())
    )

    return fix_icd10_or_opcs4(diag_df, "diagnosis")


def generate_hes_opa_diagnoses(spark: SparkSession) -> None:
    diag_df = get_hes_opa_diagnoses(spark)

    (
        diag_df.repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_opa_diagnoses)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_opa_diagnoses(spark)
