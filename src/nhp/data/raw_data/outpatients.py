"""Generate outpatients data"""

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.nhp_datasets.icbs import add_main_icb, icb_mapping
from nhp.data.nhp_datasets.local_authorities import local_authority_successors
from nhp.data.nhp_datasets.providers import read_data_with_provider
from nhp.data.raw_data.helpers import add_age_group_column, add_tretspef_grouped_column


def get_outpatients_data(spark: SparkSession) -> None:
    """Get Outpatients Data"""
    df = read_data_with_provider(spark, "hes.silver.opa")

    # Calculate icb column
    df = df.withColumn(
        "icb",
        # use the ccg of residence if available, otherwise use the ccg of responsibility
        F.coalesce(
            icb_mapping[F.col("ccg_residence")],
            icb_mapping[F.col("ccg_responsibility")],
        ),
    )

    # add main icb column
    df = add_main_icb(spark, df)
    # add the tretspef grouped column
    df = add_tretspef_grouped_column(df)
    # add age and age_group columns
    df = df.withColumn(
        "age",
        F.when(F.col("apptage") >= 7000, 0)
        .when(F.col("apptage") > 90, 90)
        .otherwise(F.col("apptage")),
    )
    df = add_age_group_column(df)
    # convert local authorities to current
    df = local_authority_successors(spark, df, "resladst_ons")

    df_primary_diagnosis = spark.read.table("hes.silver.opa_diagnoses").filter(
        F.col("diag_order") == 1
    )

    df_primary_procedure = spark.read.table("hes.silver.opa_procedures").filter(
        F.col("procedure_order") == 1
    )

    hes_opa_ungrouped = (
        df.filter(F.col("atentype").isin(["1", "2", "21", "22"]))
        .filter(F.col("sex").isin(["1", "2"]))
        .filter(F.col("age").between(0, 90))
        .withColumn(
            "is_main_icb",
            F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False),
        )
        .drop("main_icb")
        .withColumn(
            "is_surgical_specialty", F.col("tretspef").rlike("^1(?!=(80|9[012]))")
        )
        .withColumn("is_adult", (F.col("apptage") >= 18) & (F.col("apptage") <= 7000))
        .withColumn(
            "is_gp_ref",
            (F.col("refsourc") == "03") & F.col("firstatt").isin(["1", "3"]),
        )
        .withColumn(
            "is_cons_cons_ref",
            (F.col("refsourc") == "05")
            & F.col("firstatt").isin(["1", "3"])
            & F.col("sushrg").rlike("^WF0[12]B$"),
        )
        .withColumn("is_first", F.col("atentype").isin(["1", "21"]))
        .withColumn(
            "is_tele_appointment", F.col("atentype").isin(["21", "22"]).cast("int")
        )
        .withColumn(
            "has_procedures",
            ~F.col("sushrg").rlike("^(WF|U)") & (F.col("is_tele_appointment") != 1),
        )
        .withColumn("attendance", 1 - F.col("is_tele_appointment"))
        .withColumn("tele_attendance", F.col("is_tele_appointment"))
        # add in primary diagnosis and procedure columns
        .join(df_primary_diagnosis, ["attendkey", "fyear", "procode3"], "left")
        .join(df_primary_procedure, ["attendkey", "fyear", "procode3"], "left")
        .select(
            F.col("attendkey"),
            F.col("fyear"),
            F.col("procode3"),
            F.col("provider"),
            F.col("age"),
            F.col("age_group"),
            F.col("sex").cast("int"),
            F.col("imd_decile"),
            F.col("imd_quintile"),
            F.col("ethnos"),
            F.col("tretspef"),
            F.col("tretspef_grouped"),
            F.col("sitetret"),
            F.col("has_procedures"),
            F.col("sushrg"),
            F.col("resgor_ons"),
            F.col("resladst_ons"),
            F.col("lsoa11"),
            F.col("icb"),
            F.col("diagnosis").alias("primary_diagnosis"),
            F.col("procedure_code").alias("primary_procedure"),
            F.col("is_main_icb"),
            F.col("is_surgical_specialty"),
            F.col("is_adult"),
            F.col("is_gp_ref"),
            F.col("is_cons_cons_ref"),
            F.col("is_first"),
            F.col("attendance"),
            F.col("tele_attendance"),
        )
        .withColumn(
            "type",
            F.when(
                F.col("tretspef").isin(["424", "501", "505", "560"]), "maternity"
            ).otherwise(
                F.concat(
                    F.when(F.col("is_adult"), "adult").otherwise("child"),
                    F.lit("_"),
                    F.when(F.col("is_surgical_specialty"), "surgical").otherwise(
                        "non-surgical"
                    ),
                )
            ),
        )
        .withColumn(
            "group",
            F.when(F.col("has_procedures"), "procedure")
            .when(F.col("is_first"), "first")
            .otherwise("followup"),
        )
        .withColumn(
            "hsagrp", F.concat(F.lit("op_"), F.col("type"), F.lit("_"), F.col("group"))
        )
        .withColumn(
            "pod",
            F.when(F.col("has_procedures"), "op_procedure")
            .when(F.col("is_first"), "op_first")
            .otherwise("op_follow-up"),
        )
        .withColumn("ndggrp", F.col("group"))
    )

    return hes_opa_ungrouped


def generate_outpatients_data(spark: SparkSession) -> None:
    """Generate Outpatients Data"""
    hes_opa_ungrouped = get_outpatients_data(spark)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        hes_opa_ungrouped.write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("nhp.raw_data.opa")
    )


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    generate_outpatients_data(spark)
