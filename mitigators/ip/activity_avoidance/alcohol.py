# pylint: disable=line-too-long
"""Alcohol related admissions

Research into alcohol related mortality and admissions suggests there are a number of admissions
that are attributable to excess alcohol consumption which can ultimately lead to an emergency
hospital admission or even death.

Such admissions could be avoided by adopting a range of interventions including public health
strategies such as raising public awareness against the harms of excessive alcohol consumption or
minimum pricing policies or patient specific interventions such as primary care support and
increasing addiction treatment services.

## Source of Alcohol related admissions codes

Alcohol attributable condition codes are sourced from the
[2020 PHE document Alcohol-attributable fractions for England: An update][1]

These admissions have been grouped into 3 categories:

- wholly attributable to alcohol consumption (100%) (IP-AA-003)
- partially attributable to alcohol consumption (Chronic conditions) (IP-AA-002)
- partially attributable to alcohol consumption (Acute conditions) (IP-AA-001)

Whilst most activity mitigation strategies identify all spells based on the specified SQL coding in
this case the model only selects a proportion of spells based on the alcohol attributable fraction
(AAF) for that condition1. As an example the AAF for cancer of the oesophagus for males aged between
35 and 44 is 52% therefore the model randomly selects 52% of spells meeting these criteria[^2].
The AAFs are also sourced from the above referenced document.

[1]: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/958648/RELATI_1-1.pdf
[^2]: Some alcohol attributable fractions are negative (that is, evidence suggests that drinking alcohol can have a
    preventative effect for some age/sex/condition groups). The tool does not account for this.
"""
# pylint: enable=line-too-long

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from hes_datasets import any_diagnosis, diagnoses, nhp_apc
from mitigators import activity_avoidance_mitigator

spark = DatabricksSession.builder.getOrCreate()


@activity_avoidance_mitigator()
def _alcohol_wholly_attributable():
    diag_codes = [
        "E244",
        "F10",
        "G312",
        "G[67]21",
        "I426",
        "K292",
        "K70",
        "K852",
        "K860",
        "Q860",
        "R780",
        "T51[019]",
        "X[46]5",
        "Y15",
        "Y9[01]",
    ]

    return (
        nhp_apc.filter(F.col("age") >= 16)
        .admission_has(any_diagnosis, *diag_codes)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


def _alcohol_partially_attributable(condition_group):
    aaf = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .schema(
            T.StructType(
                [
                    T.StructField("condition", T.StringType(), False),
                    T.StructField("condition_group", T.StringType(), False),
                    T.StructField("sex", T.StringType(), False),
                    T.StructField("min_age", T.IntegerType(), False),
                    T.StructField("max_age", T.IntegerType(), False),
                    T.StructField("value", T.DoubleType(), False),
                    T.StructField("diagnoses", T.StringType(), False),
                    T.StructField("mortality_flag", T.IntegerType(), True),
                ]
            )
        )
        .csv("/Volumes/su_data/nhp/reference_data/alcohol_attributable_fractions.csv")
        .filter(F.col("value") > 0)
        .filter(F.col("condition_group") == condition_group)
    )

    icd10_codes = spark.read.table("su_data.reference.icd10_codes")

    aaf_mapping = (
        aaf.alias("aaf")
        # create a row per diagnosis code, replacing the need to regex join on the full dataset
        # this gives a significant performance boost in practice
        .join(
            icd10_codes,
            F.expr("icd10 RLIKE concat('^', diagnoses)"),
        )
    )

    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .alias("nhp_apc")
        .join(
            aaf_mapping.alias("aaf"),
            [
                F.col("nhp_apc.diagnosis") == F.col("aaf.icd10"),
                F.col("age") >= F.col("min_age"),
                F.col("age") <= F.col("max_age"),
                F.col("nhp_apc.sex") == F.col("aaf.sex"),
                F.col("mortality_flag").isNull()
                | ((F.col("mortality_flag") == 1) & (F.col("dismeth") == "4"))
                | ((F.col("mortality_flag") != 1) & (F.col("dismeth") != "4")),
            ],
        )
        .groupBy("epikey")
        .agg(F.max("value").alias("sample_rate"))
    )


@activity_avoidance_mitigator()
def _alcohol_partially_attributable_chronic():
    return _alcohol_partially_attributable("chronic")


@activity_avoidance_mitigator()
def _alcohol_partially_attributable_acute():
    return _alcohol_partially_attributable("acute")
