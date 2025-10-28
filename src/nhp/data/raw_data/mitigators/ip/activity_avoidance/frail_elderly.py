"""Frail Elderly Related Admissions

Older people are major users of acute health care. Some older people are at a higher risk of poor
outcomes due to frailty.
Addressing frailty remains a high priority for Healthcare systems which continue to develop services
and interventions that identify and support frail patients in order to maintain their functionality
and independence, thereby slowing or avoiding deterioration that often results in the need for
hospital care.
Frailty is also one of the core pathways where the development of virtual wards is focused and
therefore patients in this activity cohort may be suitable for admission to a frailty virtual ward.

The model identifies admissions that may be related to frailty by adapting an approach developed by
Gilbert, Thomas et al. [^1]

The model identifies all emergency admissions for patients aged over 75 and generates a total
frailty score for each spell based on diagnoses recorded for admissions during the previous 2 years.
Frailty scores for each prior admission are assigned a score taken a table of ICD-10 risk scores
within the paper above. For example a patient with 2 previous admissions where the first had a
recorded diagnosis of F00 Dementia and the second had a diagnosis of W19 unspecified fall is awarded
a score of 7.1 for the first admission and 3.2 for the second giving a total frailty score of 10.3.

Spells are categorised as either intermediate frailty risk (total score > 5) or high intermediate
risk (total score > 15).
Available breakdowns

- Intermediate level frailty (IP-AA-018)
- High level frailty (IP-AA-017)

[^1]: Gilbert, T., Neuburger, J., Kraindler, J., Keeble, E., Smith, P., Ariti, C., Arora, S.,
  Street, A., Parker, S., Roberts, H.C. and Bardsley, M., 2018. Development and validation of a
  Hospital Frailty Risk Score focusing on older people in acute care settings using electronic
  hospital records: an observational study. The Lancet, 391(10132), pp.1775-1782
"""

from functools import cache

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from nhp.data.hes_datasets import diagnoses, nhp_apc
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.table_names import table_names

spark = DatabricksSession.builder.getOrCreate()


@cache
def _frail_elderly():
    frs_ref = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .schema(
            T.StructType(
                [
                    T.StructField("icd10", T.StringType(), False),
                    T.StructField("score", T.DoubleType(), False),
                ]
            )
        )
        .csv(table_names.reference_frailty_risk_scores)
    )

    icd10_codes = spark.read.table(table_names.reference_icd10_codes)

    # make sure to use full hes table - our nhp views filter on certain columns
    # (e.g. not all providers included)
    hes_apc = (
        spark.read.table(table_names.hes_apc)
        .filter(F.col("last_episode_in_spell"))
        .withColumnRenamed("person_id_deid", "person_id")
    )

    frs_ref_mapping = (
        frs_ref.alias("frs")
        # create a row per diagnosis code, replacing the need to regex join on the full dataset
        # this gives a significant performance boost in practice
        .join(
            (
                icd10_codes.alias("icd10")
                .withColumnRenamed("icd10", "diagnosis")
                .withColumn("icd10", F.col("diagnosis").substr(1, 3))
                .select("diagnosis", "icd10")
            ),
            "icd10",
        )
        .filter(F.length(F.col("diagnosis")) > 3)
        .select("diagnosis", "score")
    )

    frs_scores = (
        diagnoses.join(frs_ref_mapping, "diagnosis")
        .join(hes_apc, "epikey")
        .withColumn("start", F.col("disdate"))
        .withColumn("end", F.date_add(F.col("disdate"), 2 * 365))
        .select("person_id", "epikey", "diagnosis", "score", "start", "end")
    )

    return (
        nhp_apc.alias("i")
        .join(
            frs_scores.alias("frs_scores").hint("range_join", 60),
            [
                F.col("i.person_id") == F.col("frs_scores.person_id"),
                # current admission is within 2 years of prior discharge
                (
                    (F.col("admidate") > F.col("start"))
                    & (F.col("admidate") <= F.col("end"))
                    | (F.col("i.epikey") == F.col("frs_scores.epikey"))
                ),
            ],
        )
        .filter(F.col("admimeth").startswith("2"))
        .filter(F.col("age") >= 75)
        .withColumn("diagnosis", F.col("diagnosis").substr(1, 3))
        .select("i.epikey", "fyear", "provider", "diagnosis", "score")
        .distinct()
        .groupby("fyear", "provider", "epikey")
        .agg(F.sum("score").alias("score"))
        .persist()
    )


@activity_avoidance_mitigator()
def _frail_elderly_intermediate():
    return (
        _frail_elderly()
        .filter(F.col("score") >= 5)
        .filter(F.col("score") <= 15)
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _frail_elderly_high():
    return (
        _frail_elderly()
        .filter(F.col("score") > 15)
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
