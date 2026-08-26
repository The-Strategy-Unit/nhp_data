from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.hes.apc import get_hes_apc
from nhp.data.table_names import table_names


def get_hes_apc_births(spark: SparkSession) -> DataFrame:
    df = get_hes_apc(spark)

    n = 9
    expr = "stack({0}, {1}) as(biresus, birordr, birstat, birweit, delmeth, delplac, delstat, gestat, sexbaby)".format(
        n,
        ", ".join(
            f"biresus_{i}, birordr_{i}, birstat_{i}, birweit_{i}, delmeth_{i}, delplac_{i}, delstat_{i}, gestat_{i}, sexbaby_{i}"
            for i in range(1, n + 1)
        ),
    )

    return df.selectExpr("epikey", "fyear", "procode3", expr).filter(
        " OR ".join(
            [
                f"{i} IS NOT NULL"
                for i in [
                    "biresus",
                    "birordr",
                    "birstat",
                    "birweit",
                    "delmeth",
                    "delplac",
                    "delstat",
                    "gestat",
                    "sexbaby",
                ]
            ]
        )
    )


def generate_hes_apc_births(spark: SparkSession) -> None:
    birth_df = get_hes_apc_births(spark)
    (
        birth_df.repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_apc_births)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_apc_births(spark)
