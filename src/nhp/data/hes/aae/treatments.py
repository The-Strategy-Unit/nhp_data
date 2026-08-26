import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def get_hes_aae_treatments(spark: SparkSession) -> DataFrame:
    df = spark.read.table(table_names.source_aae).withColumn(
        "fyear",
        F.col("period").substr(3, 4).cast("int") * 100
        + F.col("period").substr(8, 2).cast("int"),
    )

    to_melt = [f"treat_{i:02}" for i in range(1, 12)]
    melt_str = ",".join([f"'{c}', `{c}`" for c in to_melt])

    stack_expr = F.expr(
        f"stack({len(to_melt)}, {melt_str}) as (treatment_order, treatment)"
    )
    return df.select("aekey", "fyear", "procode3", stack_expr).filter(
        F.col("treatment").isNotNull()
    )


def generate_hes_aae_treatments(spark: SparkSession) -> None:
    treat_df = get_hes_aae_treatments(spark)
    (
        treat_df.repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_aae_treatments)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_aae_treatments(spark)
