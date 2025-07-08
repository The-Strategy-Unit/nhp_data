"""Generate inpatients data"""

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp_datasets.apc import apc_primary_procedures, hes_apc
from nhp_datasets.icbs import add_main_icb
from raw_data.helpers import add_tretspef_grouped_column


def get_inpatients_data(spark: SparkSession) -> None:
    """Get Inpatients Data"""
    # Spell has maternity delivery episode
    mat_delivery_spells = (
        spark.read.table("hes.silver.apc")
        .filter(F.col("fce") == 1)
        .filter(F.col("maternity_episode_type") == 1)
        .select("susspellid")
        .distinct()
        .withColumn("maternity_delivery_in_spell", F.lit(True))
    )

    df = add_main_icb(spark, hes_apc)
    df = add_tretspef_grouped_column(df)

    df_primary_diagnosis = spark.read.table("hes.silver.apc_diagnoses").filter(
        F.col("diag_order") == 1
    )

    df_primary_procedure = spark.read.table("hes.silver.apc_procedures").filter(
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
                    F.col("admimeth").startswith("3")
                    | F.col("mainspef").isin(["501", "560"])
                )
                & (F.col("age") < 56),
                "maternity",
            )
            .when(F.col("admimeth").startswith("2"), "emerg")
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
            .otherwise(None),
        )
        .withColumn("is_wla", F.col("admimeth") == "11")
        .withColumn(
            "group",
            F.when(F.col("admimeth").startswith("1"), "elective")
            .when(F.col("admimeth").startswith("3"), "maternity")
            .otherwise("non-elective"),
        )
        .filter(F.col("speldur").isNotNull())
        .filter(F.col("hsagrp").isNotNull())
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
        .tableName("nhp.raw_data.apc")
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
    spark = DatabricksSession.builder.getOrCreate()
    generate_inpatients_data(spark)


if __name__ == "__main__":
    main()
