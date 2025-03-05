"""Ambulatory Emergency Care Length of Stay Reduction


The underlying principle of Ambulatory Emergency Care (AEC) is that a significant proportion of
adult patients requiring emergency care can be managed safely and appropriately on the same day,
either without admission to a hospital bed at all, or admission for only a number of hours. 
Further information about AEC can be found on the [Ambulatory Emergency Care Network][1] website.

[1]: https://www.ambulatoryemergencycare.org.uk/

The Ambulatory Emergency Care Network has developed a directory of ambulatory emergency care for
adults which identifies 58 emergency conditions that have the potential to be managed on an
ambulatory basis, as an emergency day case. The directory provides, for each condition, an
indicative assumption about the proportion of admissions that could be managed on an ambulatory
basis.

## Available Breakdowns

The model groups these individual conditions into four categories:

- Ambulatory emergency care (low potential) (IP-EF-002)
- Ambulatory emergency care (moderate potential) (IP-EF-003)
- Ambulatory emergency care (high potential) (IP-EF-001)
- Ambulatory emergency care (very high potential) (IP-EF-004)
"""

import json
import re

import pandas as pd
from databricks.connect import DatabricksSession
from mitigators import efficiency_mitigator
from pyspark.sql import functions as F

from hes_datasets import diagnoses, nhp_apc

spark = DatabricksSession.builder.getOrCreate()


def _generate_aec_directory(group):
    with open(
        "mitigators/reference_data/aec_directory.json", "r", encoding="UTF-8"
    ) as f:
        aec_directory = json.load(f)

    ref_path = "/Volumes/strategyunit/reference/files"

    with open(f"{ref_path}/hrgs.json", "r", encoding="UTF-8") as f:
        hrgs = json.load(f)

    hrgs = [
        v3
        for v1 in hrgs.values()
        for v2 in v1["subchapters"].values()
        for v3 in v2["hrgs"].keys()
    ]

    icd10 = pd.read_csv(f"{ref_path}/icd10_codes.csv")["icd10"].to_list()

    df = pd.DataFrame(
        [
            {"diagnosis": icd, "sushrg": hrg}
            for v1 in aec_directory.values()
            for v2 in v1.values()
            if v2["group"] == group
            for hrg in [
                h for h in hrgs if re.match(f"^({'|'.join(v2['hrg_codes'])})$", h)
            ]
            for icd in [
                i for i in icd10 if re.match(f"^({'|'.join(v2['icd10_codes'])})$", i)
            ]
        ]
    )
    return spark.createDataFrame(df)


def _ambulatory_emergency_care(group):
    aec_df = _generate_aec_directory(group)

    # We could filter nhp_apc to only include speldur > 0,
    # as these are the only rows which could be affected by the model
    # but it makes it easier to keep these in for the inputs pipeline

    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .filter(F.col("age") >= 18)
        .filter(F.col("admimeth").rlike("^2[123]"))
        .filter(F.col("diag_order") == 1)
        .join(aec_df, ["diagnosis", "sushrg"], "semi")
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator()
def _ambulatory_emergency_care_low():
    return _ambulatory_emergency_care("low")


@efficiency_mitigator()
def _ambulatory_emergency_care_moderate():
    return _ambulatory_emergency_care("moderate")


@efficiency_mitigator()
def _ambulatory_emergency_care_high():
    return _ambulatory_emergency_care("high")


@efficiency_mitigator()
def _ambulatory_emergency_care_very_high():
    return _ambulatory_emergency_care("very high")
