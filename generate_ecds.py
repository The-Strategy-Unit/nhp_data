# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp inpatients
# MAGIC

# COMMAND ----------

from itertools import chain

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from datasets.icbs import icb_mapping, main_icbs
from datasets.providers import provider_successors_mapping, providers

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Has Procedure

# COMMAND ----------

df = spark.read.parquet(
    "abfss://nhse-nhp-data@sudata.dfs.core.windows.net/NHP_EC_Core/"
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
# MAGIC ### Frequent Attenders


# COMMAND ----------

freq_attenders = df.filter(F.col("ec_attendancecategory") == "1").select(
    "ec_ident", "token_person_id", "arrival_date"
)

prior_attendances = freq_attenders.select(
    "token_person_id", F.col("arrival_date").alias("prior_arrival_date")
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

hes_ecds_processed = (
    df.filter(F.col("provider").isin(providers))
    .join(freq_attenders, "ec_ident")
    .join(main_icbs, "provider", "left")
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
        ~(
            F.col("Der_EC_Investigation_All").isNotNull()
            | F.col("Der_EC_Treatment_All").isNotNull()
        ),
    )
    .groupBy(
        F.col("der_financial_year").alias("fyear"),
        F.col("provider"),
        F.col("age_at_arrival").alias("age"),
        F.col("sex"),
        F.col("der_provider_site_code").alias("sitetret"),
        F.col("ec_department_type").alias("aedepttype"),
        F.col("ec_attendancecategory").alias("attendance_category"),
        F.col("is_main_icb"),
        F.col("is_ambulance"),
        F.col("is_frequent_attender"),
        F.col("is_low_cost_referred_or_discharged"),
        F.col("is_left_before_treatment"),
        F.col("is_discharged_no_treatment"),
    )
    .count()
    .withColumnRenamed("count", "arrivals")
    .repartition("fyear", "provider")
)

# COMMAND ----------

target = (
    DeltaTable.createIfNotExists(spark)
    .tableName("su_data.nhp.ecds")
    .addColumns(hes_ecds_processed.schema)
    .execute()
)

(
    target.alias("t")
    .merge(
        hes_ecds_processed.alias("s"),
        " and ".join(
            f"t.{i} != s.{i}" for i in hes_ecds_processed.columns if i != "arrivals"
        ),
    )
    .whenMatchedUpdateAll(condition="s.arrivals != t.arrivals")
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
