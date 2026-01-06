"""Generate inpatients data"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.get_spark import get_spark
from nhp.data.nhp_datasets.apc import apc_primary_procedures, hes_apc
from nhp.data.nhp_datasets.icbs import add_main_icb
from nhp.data.raw_data.helpers import add_age_group_column, add_tretspef_grouped_column
from nhp.data.table_names import table_names


def get_inpatients_data(spark: SparkSession) -> DataFrame:
    """Get Inpatients Data"""
    # Spell has maternity delivery episode
    mat_delivery_spells = (
        spark.read.table(table_names.hes_apc)
        .filter(F.col("fce") == 1)
        .filter(F.col("maternity_episode_type") == 1)
        .select("susspellid")
        .distinct()
        .withColumn("maternity_delivery_in_spell", F.lit(True))
    )

    df = add_main_icb(spark, hes_apc)
    df = add_tretspef_grouped_column(df)
    df = add_age_group_column(df)

    df_primary_diagnosis = spark.read.table(table_names.hes_apc_diagnoses).filter(
        F.col("diag_order") == 1
    )

    df_primary_procedure = spark.read.table(table_names.hes_apc_procedures).filter(
        F.col("procedure_order") == 1
    )

    hes_apc_processed = (
        df.withColumn(
            "hsagrp",
            F.when(F.col("classpat").isin(["3", "4"]), "reg")
            .when(F.col("admimeth").isin(["82", "83"]), "birth")
            .when(F.col("mainspef") == "420", "paeds")
            .when(
                (
                    F.col("admimeth").startswith("3")  # ty: ignore[missing-argument, invalid-argument-type]
                    | F.col("mainspef").isin(["501", "560"])
                )
                & (F.col("age") < 56),
                "maternity",
            )
            .when(F.col("admimeth").startswith("2"), "emerg")  # ty: ignore[missing-argument, invalid-argument-type]
            .when(F.col("admimeth") == "81", "transfer")
            .when(
                (F.col("admimeth").isin(["11", "12", "13"]))
                & (F.col("classpat") == "1"),
                "ordelec",
            )
            .when(
                (F.col("admimeth").isin(["11", "12", "13"]))
                & (F.col("classpat") == "2"),
                "daycase",
            )
            .otherwise("unknown"),
        )
        .withColumn("is_wla", F.col("admimeth") == "11")
        .withColumn(
            "group",
            F.when(F.col("admimeth").startswith("1"), "elective")  # ty: ignore[missing-argument, invalid-argument-type]
            .when(F.col("admimeth").startswith("3"), "maternity")  # ty: ignore[missing-argument, invalid-argument-type]
            .otherwise("non-elective"),
        )
        # add has_procedure column
        .join(
            apc_primary_procedures.select(
                F.col("epikey"), F.lit(True).alias("has_procedure")
            ),
            "epikey",
            "left",
        )
        # add is_main_icb column
        .withColumn(
            "is_main_icb",
            F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False),
        )
        .drop("main_icb")
        # add in maternity_delivery_in_spell column
        .join(mat_delivery_spells, on="susspellid", how="left")
        .na.fill(False, ["has_procedure", "is_main_icb", "maternity_delivery_in_spell"])
        # add in primary diagnosis and procedure columns
        .join(df_primary_diagnosis, ["epikey", "fyear", "procode3"], "left")
        .join(df_primary_procedure, ["epikey", "fyear", "procode3"], "left")
        .select(
            F.col("epikey"),
            F.col("fyear"),
            F.col("procode3"),
            F.col("person_id_deid").alias("person_id"),
            F.col("admiage"),
            F.col("age"),
            F.col("age_group"),
            F.col("sex"),
            F.col("imd_decile"),
            F.col("imd_quintile"),
            F.col("ethnos"),
            F.col("classpat"),
            F.col("mainspef"),
            F.col("tretspef"),
            F.col("tretspef_grouped"),
            F.col("hsagrp"),
            F.col("group"),
            F.col("admidate"),
            F.col("disdate"),
            F.col("speldur"),
            F.col("epitype"),
            F.col("admimeth"),
            F.col("dismeth"),
            F.col("provider"),
            F.col("sitetret"),
            F.col("resgor_ons"),
            F.col("resladst_ons"),
            F.col("lsoa11"),
            F.col("sushrg"),
            F.col("operstat"),
            F.col("icb"),
            F.col("is_wla"),
            F.col("is_main_icb"),
            F.col("has_procedure"),
            F.col("maternity_delivery_in_spell"),
            F.col("diagnosis").alias("primary_diagnosis"),
            F.col("procedure_code").alias("primary_procedure"),
        )
        .withColumn(
            "pod",
            F.when(F.col("classpat") == "2", "ip_elective_daycase")
            .when(F.col("classpat") == "3", "ip_regular_day_attender")
            .when(F.col("classpat") == "4", "ip_regular_night_attender")
            .otherwise(F.concat(F.lit("ip_"), F.col("group"), F.lit("_admission"))),
        )
        .withColumn(
            "ndggrp",
            F.when(F.col("admimeth").isin("82", "83"), "maternity").otherwise(
                F.col("group")
            ),
        )
        .repartition("fyear", "provider")
    )

    return hes_apc_processed


def generate_inpatients_data(spark: SparkSession) -> None:
    """Generate Inpatients Data"""

    # allow schema evolution for the Delta table
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    hes_apc_processed = get_inpatients_data(spark)

    target = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_names.raw_data_apc)
        .addColumns(hes_apc_processed.schema)
        .execute()
    )

    (
        target.alias("t")
        .merge(hes_apc_processed.alias("s"), "t.epikey = s.epikey")
        .withSchemaEvolution()
        .whenMatchedUpdateAll(
            condition=" or ".join(f"t.{i} != s.{i}" for i in hes_apc_processed.columns)
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def main() -> None:
    """main method"""
    spark = get_spark()
    generate_inpatients_data(spark)
