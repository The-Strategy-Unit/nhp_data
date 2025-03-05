"""Obesity Related Admissions (IP-AA-026)

Obesity is a factor that increases the risk of developing a wide range of conditions. Reducing the
level of obesity within the population though greater investment in lifestyle management services
and health promotion would reduce the number of admissions for conditions that are at least in part
attributable to obesity. The National Audit Office publication Tackling Obesity in England 2001
identified conditions that are attributable to obesity. The publication provides an indication, for
each condition, of the proportion of cases that may be attributable to obesity.

Whilst most activity mitigation strategies identify all spells based on the specified SQL coding, in
this case the model only selects a proportion of spells based on the obesity attributable fraction
(OAF) for that condition. As an example, the OAF for hypertension is 36%. Therefore, the model
randomly selects 36% of spells meeting these criteria. The OAFs are also sourced from the above
referenced document."""

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from hes_datasets import diagnoses, nhp_apc
from mitigators import activity_avoidance_mitigator

spark = DatabricksSession.builder.getOrCreate()

import pathlib


@activity_avoidance_mitigator()
def _obesity_related_admissions():
    filename = "obesity_attributable_fractions.csv"
    path = pathlib.Path(f"reference_data/{filename}").parent.resolve()

    oaf = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .schema(
            T.StructType(
                [
                    T.StructField("diagnosis", T.StringType(), False),
                    T.StructField("fraction", T.DoubleType(), False),
                ]
            )
        )
        .csv(f"file:///{path}/{filename}")
    )

    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .filter(F.col("diag_order") == 1)
        # If running prior to 2012/13, I12 and I22 should be filtered out as they are massively
        # over-represented (coding change?)
        # .filter(~F.col("diagnosis").rlike("^I[12]2"))
        .join(oaf, ["diagnosis"])
        .select(F.col("epikey"), F.col("fraction").alias("sample_rate"))
    )
