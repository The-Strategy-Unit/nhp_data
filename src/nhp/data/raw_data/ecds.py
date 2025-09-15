"""Generate the ECDS data"""

from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.nhp_datasets.icbs import add_main_icb, icb_mapping
from nhp.data.nhp_datasets.local_authorities import local_authority_successors
from nhp.data.nhp_datasets.providers import add_provider
from nhp.data.raw_data.helpers import add_age_group_column


def get_ecds_data(spark: SparkSession) -> None:
    """Get ECDS data"""
    df = spark.read.table("hes.silver.ecds")

    df = add_provider(spark, df, "der_provider_code", "der_provider_site_code")
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

    # Add IMD fields
    imd_lookup = (
        spark.read.table("strategyunit.reference.lsoa11_to_imd19")
        .withColumnRenamed("lsoa11", "der_postcode_lsoa_2011_code")
        .withColumnRenamed("imd19_decile", "imd_decile")
        .withColumn(
            "imd_quintile", F.floor((F.col("imd_decile") - 1) / 2).cast("int") + 1
        )
    )

    df = df.join(imd_lookup, "der_postcode_lsoa_2011_code", "left")

    # Calculate icb column
    df = df.withColumn(
        "icb",
        # use the ccg of residence if available, otherwise use the ccg of responsibility
        F.coalesce(
            icb_mapping[F.col("der_postcode_ccg_code")],
            icb_mapping[F.col("Responsible_CCG_From_General_Practice")],
        ),
    )

    # Acuity mapping
    acuity_mapping = {
        "1077251000000100": "non-urgent",
        "1077241000000103": "standard",
        "1064901000000108": "urgent",
        "1064911000000105": "very-urgent",
        "1064891000000107": "immediate-resuscitation",
    }

    acuity_mapping = F.create_map([F.lit(x) for x in chain(*acuity_mapping.items())])

    # Frequent Attenders

    # for 2019/20, we need to look back one year, so use A&E data
    aae_df = (
        spark.read.table("hes.silver.aae")
        .filter(F.col("fyear") == 201819)
        .filter(F.col("aeattendcat") == "1")
        .filter(F.isnotnull("person_id_deid"))
        .select(
            F.col("person_id_deid").alias("token_person_id"),
            F.col("arrivaldate").alias("prior_arrival_date"),
        )
    )

    freq_attenders = (
        df.filter(F.col("ec_attendancecategory") == "1")
        .filter(F.isnotnull("token_person_id"))
        .select("ec_ident", "token_person_id", "arrival_date")
    )

    prior_attendances = DataFrame.unionByName(
        freq_attenders.select(
            "token_person_id", F.col("arrival_date").alias("prior_arrival_date")
        ),
        aae_df,
    ).withColumn("arrival_date_add_year", F.date_add(F.col("prior_arrival_date"), 365))

    freq_attenders = (
        freq_attenders
        # .hint("range_join", 10)
        .join(
            prior_attendances,
            [
                freq_attenders.token_person_id == prior_attendances.token_person_id,
                freq_attenders.arrival_date > prior_attendances.prior_arrival_date,
                freq_attenders.arrival_date <= prior_attendances.arrival_date_add_year,
            ],
        )
        .orderBy("ec_ident", "prior_arrival_date")
        .groupBy("ec_ident")
        .count()
        .filter(F.col("count") >= 3)
        .withColumn("is_frequent_attender", F.lit(1))
        .drop("count")
        .join(df.select("ec_ident"), "ec_ident", "right")
        .fillna(0, "is_frequent_attender")
    )

    # Mitigator code lists

    ambulance_arrival_modes = [
        "1048031000000100",
        "1048081000000101",
        "1048041000000109",
        "1048021000000102",
        "1048051000000107",
    ]

    discharged_home = [
        "989501000000106",  # Discharge from Accident and Emergency service with advice for follow up treatment by general practitioner (procedure)
        "3780001",  # Routine patient disposition, no follow-up planned
    ]

    left_before_treated = [
        "1066301000000103",  # Left care setting before initial assessment (finding)
        "1066311000000101",  # Left care setting after initial assessment (finding)
        "1066321000000107",  # Left care setting before treatment completed (finding)
    ]

    # add main icb column
    df = add_main_icb(spark, df)
    # add age and age_group columns
    df = df.withColumn(
        "age",
        F.when(F.col("age_at_arrival") > 90, 90)
        .otherwise(F.col("age_at_arrival"))
        .cast("int"),
    )
    df = add_age_group_column(df)
    # convert local authorities to current
    df = local_authority_successors(spark, df, "local_authority_district")

    hes_ecds_ungrouped = (
        df.join(freq_attenders, "ec_ident")
        .filter(F.col("sex").isin(["1", "2"]))
        .filter(F.col("age").between(0, 90))
        .withColumn("is_adult", F.col("age") >= 18)
        .withColumn(
            "fyear", F.regexp_replace(F.col("der_financial_year"), "/", "").cast("int")
        )
        .withColumn("acuity", acuity_mapping[F.col("ec_acuity_snomed_ct")])
        .withColumn(
            "is_main_icb",
            F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False),
        )
        .drop("main_icb")
        .withColumn(
            "is_ambulance",
            F.col("EC_Arrival_Mode_SNOMED_CT").isin(ambulance_arrival_modes),
        )
        .withColumn(
            "is_low_cost_referred_or_discharged",
            F.col("Discharge_Follow_Up_SNOMED_CT").isin(discharged_home)
            & F.col("SUS_HRG_Code").rlike("^VB(0[69]|1[01])Z$"),
        )
        .withColumn(
            "is_left_before_treatment",
            F.col("EC_Discharge_Status_SNOMED_CT").isin(left_before_treated),
        )
        .withColumn(
            "is_discharged_no_treatment",
            (
                (
                    F.col("Der_EC_Investigation_All").isNull()
                    | (F.col("Der_EC_Investigation_All") == "1088291000000101")
                )
                & (
                    F.col("Der_EC_Treatment_All").isNull()
                    | (F.col("Der_EC_Treatment_All") == "183964008")
                )
            ),
        )
        # for the boolean columns, default to False if null
        .fillna(
            {
                k: False
                for k in [
                    "is_ambulance",
                    "is_low_cost_referred_or_discharged",
                    "is_left_before_treatment",
                    "is_discharged_no_treatment",
                ]
            }
        )
        .withColumn(
            "primary_treatment",
            F.regexp_extract(F.col("Der_EC_Treatment_All"), "^(\\d+),", 1),
        )
        .select(
            F.col("ec_ident").cast("string").alias("key"),
            F.lit("ecds").alias("data_source"),
            F.col("fyear"),
            F.col("der_provider_code").alias("procode3"),
            F.col("provider"),
            F.col("age"),
            F.col("age_group"),
            F.col("sex").cast("int"),
            F.col("imd_decile"),
            F.col("imd_quintile"),
            F.col("Ethnic_Category").alias("ethnos"),
            F.col("der_provider_site_code").alias("sitetret"),
            F.col("ec_department_type").alias("aedepttype"),
            F.col("ec_attendancecategory").alias("attendance_category"),
            F.col("arrival_date").cast("date").alias("arrival_date"),
            F.col("acuity"),
            F.col("government_office_region").alias("resgor_ons"),
            F.col("local_authority_district").alias("resladst_ons"),
            F.col("der_postcode_lsoa_2011_code").alias("lsoa11"),
            F.col("icb"),
            F.col("is_main_icb"),
            F.col("is_adult"),
            F.col("is_ambulance"),
            F.col("is_frequent_attender").cast("boolean"),
            F.col("is_low_cost_referred_or_discharged"),
            F.col("is_left_before_treatment"),
            F.col("is_discharged_no_treatment"),
            F.col("EC_Chief_Complaint_SNOMED_CT").alias("primary_diagnosis"),
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
        .withColumn("tretspef_grouped", F.lit("Other"))
        .withColumn("pod", F.concat(F.lit("aae_type-"), F.col("aedepttype")))
        .withColumn("ndggrp", F.col("group"))
        .repartition("fyear", "provider")
    )

    return hes_ecds_ungrouped


def generate_ecds_data(spark: SparkSession) -> None:
    """Generate ECDS data"""
    hes_ecds_ungrouped = get_ecds_data(spark)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        hes_ecds_ungrouped.write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("nhp.raw_data.ecds")
    )


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    generate_ecds_data(spark)
