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

from datetime import date
from functools import cache

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from hes_datasets import diagnoses, nhp_apc
from mitigators import activity_avoidance_mitigator

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
        .csv("/Volumes/su_data/nhp/reference_data/frailty_risk_scores.csv")
    )

    icd10_codes = spark.read.table("su_data.reference.icd10_codes")

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
            "icd10"
        )
        .filter(F.length(F.col("diagnosis")) > 3)
        .select("diagnosis", "score")
    )

    diags = (
        diagnoses
        .join(frs_ref_mapping, "diagnosis")
        .groupBy(["fyear", "procode3", "epikey"])
        .agg(F.sum("score").alias("score"))
    )

    frs_scores = nhp_apc.filter(F.col("disdate") < date(2100, 1, 1)).join(
        diags, ["epikey", "fyear"]
    )

    prior = (
        frs_scores.alias("prior")
        .withColumn("start", F.col("disdate"))
        .withColumn("end", F.date_add(F.col("disdate"), 2 * 365))
        .select(
            F.col("person_id"),
            F.col("start"),
            F.col("end"),
            F.col("score").alias("prior_score"),
        )
    )

    return (
        frs_scores.alias("current")
        .filter(F.col("admimeth").startswith("2"))
        .filter(F.col("age") >= 75)
        .join(
            prior.hint("range_join", 60),
            [
                F.col("current.person_id") == F.col("prior.person_id"),
                # current admission is within 2 years of prior discharge
                F.col("admidate") > F.col("start"),
                F.col("admidate") <= F.col("end"),
            ],
        )
        .orderBy("current.person_id", "start", "end", "admidate")
        .groupBy("epikey", "fyear", "score")
        .agg(F.sum("prior_score").alias("prior_score"))
        .withColumn("score", F.col("prior_score") + F.col("score"))
        .select(
            F.col("epikey").alias("epikey"),
            F.col("fyear").alias("fyear"),
            F.col("score"),
        )
        .persist()
    )


@activity_avoidance_mitigator()
def _frail_elderly_intermediate():
    return (
        _frail_elderly()
        .filter(F.col("score") >= 5)
        .filter(F.col("score") <= 15)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _frail_elderly_high():
    return (
        _frail_elderly()
        .filter(F.col("score") > 15)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
