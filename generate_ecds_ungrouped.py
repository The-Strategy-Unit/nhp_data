# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp ecds (aae)
# MAGIC

# COMMAND ----------

from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from nhp_datasets.icbs import icb_mapping, main_icbs
from nhp_datasets.providers import get_provider_successors_mapping, providers

spark = DatabricksSession.builder.getOrCreate()
provider_successors_mapping = get_provider_successors_mapping()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ECDS source data

# COMMAND ----------

df = (
    spark.read.parquet("abfss://nhse-nhp-data@sudata.dfs.core.windows.net/NHP_EC_Core/")
    .filter(F.col("sex").isin(["1", "2"]))
    .filter(F.col("deleted") == 0)
)

df = df.select([F.col(c).alias(c.lower()) for c in df.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate provider column

# COMMAND ----------

df = df.withColumn(
    "provider",
    F.when(F.col("der_provider_site_code") == "RW602", "R0A")
    .when(F.col("der_provider_site_code") == "RM318", "R0A")
    .otherwise(provider_successors_mapping[F.col("der_provider_code")]),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate icb column

# COMMAND ----------

df = df.withColumn("icb", icb_mapping[F.col("der_postcode_ccg_code")])


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create ECDS Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Acuity mapping

# COMMAND ----------
acuity_mapping = {
    "1077251000000100": "non-urgent",
    "1077241000000103": "standard",
    "1064901000000108": "urgent",
    "1064911000000105": "very-urgent",
    "1064891000000107": "immediate-resuscitation",
}

acuity_mapping = F.create_map([F.lit(x) for x in chain(*acuity_mapping.items())])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Frequent Attenders


# COMMAND ----------

# for 2019/20, we need to look back one year, so use A&E data
aae_df = (
    spark.read.table("hes.silver.aae")
    .filter(F.col("fyear") == 201819)
    .filter(F.col("aeattendcat") == "1")
    .filter(F.col("person_id_deid").isNotNull())
    .select(
        F.col("person_id_deid").alias("token_person_id"),
        F.col("arrivaldate").alias("prior_arrival_date"),
    )
)

freq_attenders = (
    df.filter(F.col("ec_attendancecategory") == "1")
    .filter(F.col("token_person_id").isNotNull())
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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Mitigator code lists


# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Extract data


# COMMAND ----------

hes_ecds_ungrouped = (
    df.filter(F.col("provider").isin(providers))
    .join(freq_attenders, "ec_ident")
    .join(main_icbs, "provider", "left")
    .withColumn(
        "age",
        F.when(F.col("age_at_arrival") > 90, 90)
        .otherwise(F.col("age_at_arrival"))
        .cast("int"),
    )
    .withColumn("is_adult", F.col("age") >= 18)
    .withColumn(
        "fyear", F.regexp_replace(F.col("der_financial_year"), "/", "").cast("int")
    )
    .withColumn("acuity", acuity_mapping[F.col("ec_acuity_snomed_ct")])
    .withColumn(
        "is_main_icb", F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False)
    )
    .drop("main_icb")
    .withColumn(
        "is_ambulance", F.col("EC_Arrival_Mode_SNOMED_CT").isin(ambulance_arrival_modes)
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
        F.col("ec_ident"),
        F.col("fyear"),
        F.col("der_provider_code").alias("procode3"),
        F.col("provider"),
        F.col("age"),
        F.col("sex").cast("int"),
        F.col("der_provider_site_code").alias("sitetret"),
        F.col("ec_department_type").alias("aedepttype"),
        F.col("ec_attendancecategory").alias("attendance_category"),
        F.col("acuity"),
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
    .repartition("fyear", "provider")
)

# COMMAND ----------

(
    hes_ecds_ungrouped.write.partitionBy("fyear", "provider")
    .mode("overwrite")
    .saveAsTable("su_data.nhp.ecds_ungrouped")
)
