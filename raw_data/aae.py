"""Generate the AAE data"""

from itertools import chain

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import *  # pylint: disable-all

from nhp_datasets.icbs import add_main_icb, icb_mapping
from nhp_datasets.providers import read_data_with_provider


def get_aae_data(spark: SparkContext) -> None:
    """Get AAE data

    inserts into the ECDS table, before ECDS dataset was available"""

    # aae data doesn't contain sitetret - use procode3 (will have 0 effect)
    df = read_data_with_provider(
        spark, "hes.silver.aae", sitetret_col="procode3"
    ).filter(F.col("fyear") < 201920)

    # Frequent Attendners
    freq_attenders = (
        df.filter(F.col("aeattendcat") == "1")
        .filter(F.col("person_id_deid").isNotNull())
        .select("aekey", "person_id_deid", "arrivaldate")
    )

    prior_attendances = freq_attenders.select(
        "person_id_deid", F.col("arrivaldate").alias("prior_arrival_date")
    ).withColumn("arrival_date_add_year", F.date_add(F.col("prior_arrival_date"), 365))

    freq_attenders = (
        freq_attenders
        # .hint("range_join", 10)
        .join(
            prior_attendances,
            [
                freq_attenders.person_id_deid == prior_attendances.person_id_deid,
                freq_attenders.arrivaldate > prior_attendances.prior_arrival_date,
                freq_attenders.arrivaldate <= prior_attendances.arrival_date_add_year,
            ],
        )
        .orderBy("aekey", "prior_arrival_date")
        .groupBy("aekey")
        .count()
        .filter(F.col("count") >= 3)
        .withColumn("is_frequent_attender", F.lit(1))
        .drop("count")
        .join(df.select("aekey"), "aekey", "right")
        .fillna(0, "is_frequent_attender")
    )

    # Is Discharged with No Treatment or Investigations

    df_treatments = (
        spark.read.table("hes.silver.aae_treatments")
        .filter(F.col("treatment") != "24")
        .select("aekey", "fyear", "procode3")
    )

    df_investigations = (
        spark.read.table("hes.silver.aae_investigations")
        .filter(~F.col("investigation").isin(["22", "99"]))
        .select("aekey", "fyear", "procode3")
    )

    df_treatments_or_investigations = (
        DataFrame.unionByName(df_treatments, df_investigations)
        .distinct()
        .withColumn("is_discharged_no_treatment", F.lit(False))
    )

    # Primary Diagnosis/Treatment

    df_pri_diag = (
        spark.read.table("hes.silver.aae_diagnoses")
        .filter(F.col("diag_order") == 1)
        .withColumn("diagnosis", F.col("diagnosis").substr(0, 2))
        .filter(F.col("diagnosis").rlike("^[0-3][0-9]$"))
        .drop("diagnosis_order")
        .withColumnRenamed("diagnosis", "primary_diagnosis")
    )

    df_pri_treat = (
        spark.read.table("hes.silver.aae_treatments")
        .filter(F.col("treatment_order") == 1)
        .withColumn("treatment", F.col("treatment").substr(0, 2))
        .filter(F.col("treatment").rlike("^([0-4][0-9]|5[0-7]|99)$"))
        .drop("treatment_order")
        .withColumnRenamed("treatment", "primary_treatment")
    )

    # Calculate icb column
    df = df.withColumn("icb", icb_mapping[F.col("ccg_residence")])

    # add main icb column
    df = add_main_icb(spark, df)

    hes_aae_ungrouped = (
        df.filter(F.col("sex").isin(["1", "2"]))
        .withColumn(
            "age",
            F.when(F.col("activage") >= 7000, 0)
            .when(F.col("activage") > 90, 90)
            .otherwise(F.col("activage")),
        )
        .filter(F.col("age") <= 120)
        .withColumn(
            "is_main_icb",
            F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False),
        )
        .drop("main_icb")
        .withColumn("is_adult", F.col("age") >= 18)
        .withColumn("is_ambulance", F.col("aearrivalmode") == "1")
        .withColumn(
            "is_low_cost_referred_or_discharged",
            F.col("sushrg").rlike("^VB(0[69]|1[01])Z$")
            & F.col("aeattenddisp").rlike("^0[23]$"),
        )
        .withColumn("is_left_before_treatment", F.col("aeattenddisp") == "12")
        .join(freq_attenders, "aekey")
        .join(
            df_treatments_or_investigations, ["procode3", "fyear", "aekey"], how="left"
        )
        .fillna(True, ["is_discharged_no_treatment"])
        .withColumn(
            "is_discharged_no_treatment",
            F.when(F.col("aeattenddisp") != "03", False).otherwise(
                F.col("is_discharged_no_treatment")
            ),
        )
        .join(df_pri_diag, ["procode3", "fyear", "aekey"], how="left")
        .join(df_pri_treat, ["procode3", "fyear", "aekey"], how="left")
        .select(
            F.col("aekey").alias("key"),
            F.lit("aae").alias("data_source"),
            F.col("fyear"),
            F.col("procode3"),
            F.col("provider"),
            F.col("age"),
            F.col("sex").cast("int"),
            F.col("imd_decile"),
            F.col("imd_quintile"),
            F.col("provider").alias("sitetret"),
            F.col("aedepttype"),
            F.col("aeattendcat").alias("attendance_category"),
            F.col("arrivaldate").alias("arrival_date"),
            F.col("resgor_ons"),
            F.col("resladst_ons"),
            F.col("lsoa11"),
            F.col("icb"),
            F.col("is_main_icb"),
            F.col("is_adult"),
            F.col("is_ambulance"),
            F.col("is_frequent_attender").cast("boolean"),
            F.col("is_low_cost_referred_or_discharged"),
            F.col("is_left_before_treatment"),
            F.col("is_discharged_no_treatment"),
            F.col("primary_diagnosis"),
            F.col("primary_treatment"),
            F.lit(1).alias("arrival"),
        )
        .withColumn(
            "group", F.when(F.col("is_ambulance"), "ambulance").otherwise("walk-in")
        )
        .withColumn(
            "type",
            F.concat(
                F.when(F.col("is_adult"), "adult").otherwise("child"),
                F.lit("_"),
                F.col("group"),
            ),
        )
        .withColumn("hsagrp", F.concat(F.lit("aae_"), F.col("type")))
        .withColumn("tretspef", F.lit("Other"))
        .repartition("fyear", "provider")
    )

    return hes_aae_ungrouped


def generate_aae_data(spark: SparkContext) -> None:
    """Generate AAE data"""
    hes_aae_ungrouped = get_aae_data(spark)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        hes_aae_ungrouped.write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("nhp.raw_data.ecds")
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    generate_aae_data(spark)
