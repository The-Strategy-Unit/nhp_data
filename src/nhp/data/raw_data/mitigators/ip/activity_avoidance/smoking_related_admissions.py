"""Smoking Related Admissions (IP-AA-029)

Smoking is the biggest single cause of preventable death and ill-health within England. Reducing
smoking prevalence, through investment in smoking cessation services and public health interventions
aimed at reducing take up, will reduce admissions for a wide range of smoking attributable
conditions. In addition, historical measures such as national smoking bans will continue to have an
impact in future.

## Source of Smoking Related Admissions codes

The 2018 Royal college of physicians report [Hiding in plain sight][1] provides a list of conditions
and their associated ICD-10 codes that can be attributable to smoking. The model uses this list to
identify spells in the model that could be avoided.

Whilst most activity mitigation strategies identify all spells based on the specified SQL coding, in
this case the model only selects a proportion of spells based on the smoking attributable fraction
(SAF) for that condition. As an example, the AAF for cancer of the larynx for males is 43%.
Therefore, the model randomly selects 43% of spells meeting these criteria. The SAFs are also
sourced from the above referenced document.

[1]: https://www.rcplondon.ac.uk/projects/outputs/hiding-plain-sight-treating-tobacco-dependency-nhs
"""

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from nhp.data.hes_datasets import diagnoses, nhp_apc
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.reference_data import get_reference_file_path
from nhp.data.table_names import table_names

spark = DatabricksSession.builder.getOrCreate()


@activity_avoidance_mitigator()
def _smoking():
    filename = get_reference_file_path("smoking_attributable_fractions.csv")

    saf = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .schema(
            T.StructType(
                [
                    T.StructField("diagnoses", T.StringType(), False),
                    T.StructField("sex", T.IntegerType(), False),
                    T.StructField("value", T.DoubleType(), False),
                ]
            )
        )
        .csv(f"file:///{filename}")
    )

    icd10_codes = spark.read.table(table_names.reference_icd10_codes)

    saf_mapping = saf.join(
        icd10_codes, F.expr("icd10 RLIKE concat('^', diagnoses)")
    ).select(F.col("icd10").alias("diagnosis"), F.col("sex"), F.col("value"))

    return (
        nhp_apc.join(diagnoses.filter(F.col("diag_order") == 1), ["epikey", "fyear"])
        .join(
            saf_mapping,
            ["diagnosis", "sex"],
        )
        .groupBy("fyear", "provider", "epikey")
        .agg(F.max("value").alias("sample_rate"))
    )
