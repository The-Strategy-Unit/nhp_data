from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from nhp.data.get_spark import get_spark
from nhp.data.hes.apc import get_hes_apc
from nhp.data.hes.fix_icd10_or_opcs4 import fix_icd10_or_opcs4
from nhp.data.table_names import table_names


def _hes_apc_episodes_remove_normalised_columns(df: DataFrame) -> DataFrame:
    """Remove normalised columns

    These columns are normalised in other tasks in the workflow."""

    return df.drop(
        *(
            [
                f"{c}_{i}"
                for i in range(1, 10)
                for c in [
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
            + [f"diag_{i:02}" for i in range(1, 21)]
            + [f"{c}_{i:02}" for i in range(1, 25) for c in ["opdate", "opertn"]]
        )
    )


def _hes_apc_episodes_add_imd_decile(df: DataFrame) -> DataFrame:
    """Recreate the IMD decile

    IMD04 on activity up to and including 2006-07
    IMD07 on activity between 2007-08 and 2009-10
    IMD10 on activity from 2010-11 and M10 2022-23
    IMD19 from M11 2022-23"""

    imdrk_to_ntile = (
        df.select("fyear", "imd04rk")
        .filter(F.col("imd04rk").isNotNull())
        .distinct()
        .withColumn(
            "imd_decile",
            F.ntile(10).over(Window.partitionBy("fyear").orderBy("imd04rk")),
        )
        .withColumn(
            "imd_quintile",
            F.ntile(5).over(Window.partitionBy("fyear").orderBy("imd04rk")),
        )
    )

    return (
        df.drop("imd04_decile")
        .join(imdrk_to_ntile, ["fyear", "imd04rk"], "left")
        .withColumn(
            "imd_version",
            F.when(F.col("imd04rk").isNull(), F.lit(None).cast("string"))
            .when(F.col("fyear") >= 202324, F.lit("IMD19"))
            # change happened in M10 2022/23, e.g. January 2023
            .when(F.year(F.col("admidate")) == 2023, F.lit("IMD19"))
            .when(F.col("fyear") >= 201011, F.lit("IMD10"))
            .when(F.col("fyear") >= 200708, F.lit("IMD07"))
            .otherwise(F.lit("IMD04")),
        )
    )


def _hes_apc_episodes_add_last_episode_in_spell(df: DataFrame) -> DataFrame:
    """Create last episode in spell column

    Uses the methodology in [Methodology to create provider and CIP spells from HES APC data](https://files.digital.nhs.uk/B6/4A484B/Methodology%20to%20create%20provider%20and%20CIP%20spells%20from%20HES%20APC%20data%20v2.pdf)

    > Episodes that have the same `TOKEN_PERSON_ID`, `ADMIDATE`, `PROCODET_MAPPED` and `PROVSPNOPS` are considered to be in the same provider spell.
    > Regular attender episodes (`CLASSPAT` = `"3"` and `"4"`) are considered as separate units of care that should not be linked to other episodes and therefore are excluded from the episode ordering criteria shown below – they form single episode provider spells.
    >
    > Episodes within a provider spell are sorted using the following criteria:
    > 1. EPISTART
    > 2. EPIORDER
    > 3. EPIEND
    > 4. EPIKEY
    >
    > The order of episodes within the spell is indicated by a derived field called P_SPELL_EPIORDER.
    > In most cases this field should match the provider submitter episode order (EPIORDER) but in a small number of cases data quality issues have caused this to be different.
    >
    > ...
    >
    > These episodes are flagged using the derived field `P_SPELL_LAST_EPISODE` = `"Y"`.
    > This flag is applied only on "closed spells" (i.e. spells with an episode containing a valid discharge date) on the episode with the highest `P_SPELL_EPIORDER`"""

    w = Window.partitionBy(["susspellid"]).orderBy(
        F.desc("epistart"), F.desc("epiorder"), F.desc("epiend"), F.desc("epikey")
    )

    last_episode_in_spell = (
        df.filter(F.col("epistat") == "3")
        .filter(F.col("admidate").isNotNull())
        .filter(F.col("dismeth") != "8")
        .filter(F.col("disdate").isNotNull())
        .filter(F.col("susspellid") != "-1")
        .filter(F.col("susspellid").isNotNull())
        .withColumn("p_rev_spell_epiorder", F.row_number().over(w))
        .filter(F.col("p_rev_spell_epiorder") == 1)
        .select("epikey")
        .withColumn("last_episode_in_spell", F.lit(True))
    )

    return df.join(last_episode_in_spell, "epikey", "left").na.fill(
        False, ["last_episode_in_spell"]
    )


def _hes_apc_add_maternity_episode_type(df: DataFrame):
    epitype_2_5 = F.col("epitype").isin(["2", "5"])
    epitype_3_6 = F.col("epitype").isin(["3", "6"])
    delplac_1_5_6 = F.col("delplac_1").isin(["1", "5", "6"])
    classpat_3_4 = F.col("classpat").isin(["3", "4"])
    epistat_3 = F.col("epistat") == "3"
    delmeth_not_null = F.col("delmeth_1").isNotNull()

    return df.withColumn(
        "maternity_episode_type",
        F.when(epitype_2_5 & ~delplac_1_5_6 & ~classpat_3_4 & epistat_3, 1)
        .when(epitype_3_6 & ~delplac_1_5_6 & ~classpat_3_4 & epistat_3, 2)
        .when(epitype_2_5 & delplac_1_5_6 & ~classpat_3_4 & epistat_3, 3)
        .when(epitype_3_6 & delplac_1_5_6 & ~classpat_3_4 & epistat_3, 4)
        .when(delmeth_not_null, 9)
        .otherwise(99),
    )


def get_hes_apc_episodes(spark: SparkSession) -> DataFrame:
    df = get_hes_apc(spark)

    fns = [
        lambda x: fix_icd10_or_opcs4(x, "cause"),
        _hes_apc_add_maternity_episode_type,  # ensure this happens before removing normalised columns, as it uses delmeth_1 and delplac_1
        _hes_apc_episodes_remove_normalised_columns,
        _hes_apc_episodes_add_imd_decile,
        _hes_apc_episodes_add_last_episode_in_spell,
    ]
    return reduce(lambda x, fn: fn(x), fns, df)


def generate_hes_apc_episodes_data(spark: SparkSession) -> None:
    df = get_hes_apc_episodes(spark)

    (
        df.select(*sorted(df.columns))
        .repartition("procode3")
        .write.option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy(["fyear", "procode3", "last_episode_in_spell"])
        .saveAsTable(table_names.hes_apc)
    )


def main() -> None:
    spark = get_spark()
    generate_hes_apc_episodes_data(spark)
