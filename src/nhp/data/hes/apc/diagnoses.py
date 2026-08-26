import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from nhp.data.get_spark import get_spark
from nhp.data.hes.apc import get_hes_apc
from nhp.data.hes.fix_icd10_or_opcs4 import fix_icd10_or_opcs4
from nhp.data.table_names import table_names


def get_hes_apc_diagnoses(spark: SparkSession) -> DataFrame:
    to_melt = [f"diag_{i:02}" for i in range(1, 21)]
    melt_str = ",".join([f"'{c}', `{c}`" for c in to_melt])

    stack_expr = F.expr(f"stack({len(to_melt)}, {melt_str}) as (diag_order, diagnosis)")

    df = (
        get_hes_apc(spark)
        .select("epikey", "fyear", "procode3", stack_expr)
        .filter(F.col("diagnosis").isNotNull())
        .withColumn(
            "diag_order", F.substring(F.col("diag_order"), 6, 2).cast(IntegerType())
        )
    )

    return fix_icd10_or_opcs4(df, "diagnosis")


def generate_hes_apc_diagnoses(spark: SparkSession) -> None:
    diag_df = get_hes_apc_diagnoses(spark)
    (
        diag_df.repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_apc_diagnoses)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_apc_diagnoses(spark)
