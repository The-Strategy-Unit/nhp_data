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

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from nhp.data.hes_datasets import any_diagnosis, diagnoses, nhp_apc
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.reference_data import load_json


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
        .admission_has(any_diagnosis, *diag_codes)  # ty: ignore[call-non-callable]
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


def _alcohol_partially_attributable(condition_group):
    spark = DatabricksSession.builder.getOrCreate()
    assert hasattr(spark, "sparkContext"), (
        "sparkContext is not available on the SparkSession object."
    )
    sc = spark.sparkContext

    icd = spark.read.table("strategyunit.reference.icd10_codes")

    aaf = load_json("alcohol_fractions")

    ages = list(zip(aaf["ages"], aaf["ages"][1:] + [10000]))
    aaf = [
        {
            "regex": v["regex"],
            "sex": s1,
            "type": k,
            "min_age": min_age,
            "max_age": max_age - 1,
            "value": age_value,
        }
        for i in [
            j if "mortality" in j else {"mortality": j, "morbidity": j}
            for i in aaf["data"][condition_group].values()
            for j in i.values()
        ]
        for k, v in i.items()
        for s1, s2 in [(1, "male"), (2, "female")]
        if s2 in v
        for (min_age, max_age), age_value in zip(ages, v[s2])
        if 0 < age_value <= 1
    ]
    aaf = (
        spark.read.json(sc.parallelize([aaf]))
        .join(icd.select("icd10"), F.expr("icd10 rlike regex"))
        .persist()
    )

    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .withColumn(
            "type", F.when(F.col("dismeth") == 4, "mortality").otherwise("morbidity")
        )
        .alias("nhp_apc")
        .join(
            aaf.alias("aaf").hint("range_join", 10),
            [
                F.col("nhp_apc.diagnosis") == F.col("aaf.icd10"),
                F.col("age") >= F.col("min_age"),
                F.col("age") <= F.col("max_age"),
                F.col("nhp_apc.sex") == F.col("aaf.sex"),
                F.col("nhp_apc.type") == F.col("aaf.type"),
            ],
        )
        .groupBy("fyear", "provider", "epikey")
        .agg(F.max("value").alias("sample_rate"))
    )


@activity_avoidance_mitigator()
def _alcohol_partially_attributable_chronic():
    return _alcohol_partially_attributable("chronic")


@activity_avoidance_mitigator()
def _alcohol_partially_attributable_acute():
    return _alcohol_partially_attributable("acute")
