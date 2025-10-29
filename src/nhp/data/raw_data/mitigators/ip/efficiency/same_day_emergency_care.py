"""Same Day Emergency Care Conversion

The [Ambulatory Emergency Care Network](https://www.nhselect.nhs.uk/improvement-collaboratives/Clinical-networks/AEC) has published a Directory of Ambulatory Emergency Care for adults, 6th ed (2018) which identifies emergency conditions that have the potential to be managed as an emergency day case delivered in an SDEC unit.
We have grouped these conditions into four mitigator cohorts, those that the network suggest have a:

* low potential to be managed on an ambulatory basis (10-30% of cases)
* moderate potential to be managed on an ambulatory basis (30-60% of cases)
* high potential to be managed on an ambulatory basis (60-90% of cases)
* very high potential to be managed on an ambulatory basis (90-100% of cases)

The spells identified by these mitigators exclude any with a length of stay (LOS) of 3 days and over, as these spells are considered less likely to be treatable within a day.
This mitigator allows Trusts to make an assumption about what proportion of the identified spells they think will, in future, be delivered within an SDEC facility and not as an admitted patient.
The model removes this quantity of spells from the inpatients data and adds them to the A&E outputs as type 5 (SDEC) attendances, enabling clear identification and quantification of future SDEC activity with the model outputs.

Note: these mitigators replace the previous set of Ambulatory Emergency Care (AEC) efficiency mitigators that adjusted the LOS of selected spells to 0 LOS but kept them within the inpatients (IP) dataset.
The change reflects the new requirement for Trusts to record SDEC activity within the ECDS dataset.

## Available Breakdowns

The model groups these individual conditions into four categories:

- Same day emergency care (low potential) (IP-EF-029)
- Same day emergency care (moderate potential) (IP-EF-030)
- Same day emergency care (high potential) (IP-EF-028)
- Same day emergency care (very high potential) (IP-EF-031)
"""

import json
import re

import pandas as pd
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from nhp.data.hes_datasets import diagnoses, nhp_apc
from nhp.data.raw_data.mitigators import efficiency_mitigator
from nhp.data.raw_data.mitigators.reference_data import load_json
from nhp.data.table_names import table_names

spark = DatabricksSession.builder.getOrCreate()


def _generate_aec_directory(group):
    aec_directory = load_json("aec_directory")

    hrgs = [
        v3
        for v1 in load_json("hrgs").values()
        for v2 in v1["subchapters"].values()
        for v3 in v2["hrgs"].keys()
    ]

    icd10 = (
        spark.read.table(table_names.reference_icd10_codes)
        .select("icd10")
        .toPandas()["icd10"]
        .to_list()
    )

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


def _same_day_emergency_care(group):
    aec_df = _generate_aec_directory(group)

    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .filter(F.col("age") >= 18)
        .filter(F.col("admimeth").rlike("^2[123]"))
        .filter(F.col("diag_order") == 1)
        .filter(F.col("speldur") <= 2)
        .join(aec_df, ["diagnosis", "sushrg"], "semi")
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator()
def _same_day_emergency_care_low():
    return _same_day_emergency_care("low")


@efficiency_mitigator()
def _same_day_emergency_care_moderate():
    return _same_day_emergency_care("moderate")


@efficiency_mitigator()
def _same_day_emergency_care_high():
    return _same_day_emergency_care("high")


@efficiency_mitigator()
def _same_day_emergency_care_very_high():
    return _same_day_emergency_care("very high")
