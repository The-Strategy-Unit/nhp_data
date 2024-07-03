"""Day Procedures

Day surgery is now established practice with rates continuing to increase. Due to advances in
anaesthesia and surgical techniques, it is the standard pathway of care for many complex patients
and procedures traditionally treated through inpatient pathways.

Day surgery represents high-quality patient care with excellent patient satisfaction. Shorter
hospital stays and early mobilization reduce rates of hospital-acquired infection and venous
thromboembolism. Patients overwhelmingly endorse day surgery, with smaller waiting times, less risk
of cancellation, lower rates of infection, and the preference of their own surroundings to
convalesce.

The tool identifies elective spells with a procedure where there is potential to deliver that
activity in a different setting. It classifies those spells into four types based on an analysis of
national data:

- Day Procedures: Usually performed in Outpatients (IP-EF-008) -
    Procedures that are usually (>50% of all spells/attendances nationally) carried out in an
    outpatient procedure room
- Day Procedures: Usually performed as a Daycase (IP-EF-007) -
    Procedures that are usually (>50% of all spells/attendances nationally) carried out in a day
    case setting
- Day Procedures: Occasionally performed in Outpatients (IP-EF-006) -
    Procedures occasionally (>5% of all spells/attendances nationally, excluding any procedures in
    Type A or B) carried out as a day case or in a procedure room (outpatient) setting
- Day Procedures: Occasionally performed as a Daycase (IP-EF-005) -
    Procedures occasionally (>5% of all spells/attendances nationally, excluding any procedures in
    Type A or B) carried out in a day case setting.

The input tool requires users to set assumptions about the future proportion of cases undertaken in
the "non target" setting for each of the above types.

For Usually or Occasionally performed in Outpatients spells, the assumption would be the assumed
future proportion of spells undertaken as an inpatient or daycase (or the % reduction if setting the
assumption relatively).

For Usually or Occasionally performed as a Dayscase spells, the assumption would be the assumed
future proportion of spells undertaken as an ordinary inpatient (or the % reduction if setting the
assumption relatively).

## Classification methodology

Some elective procedures could be performed in a daycase setting in the future, and some
elective/daycase procedures could be performed in an outpatient procedure room.

We have counted all activity in 2019/20 where the primary procedure code starts with an A through T,
split into one of three groups:

* elective inpatients: admission method starting with a 1, patient classification of 1
* daycases: admission method starting with a 1, patient classification of 2
* outpatients: outpatient attendance with a HRG that does not start with WF or U

We then have a table that contains a row per procedure, a column for each group containing the
counts for that group, and a total column. For example:

| procedure_code | ip   | dc   | op   | total |
| :------------- | ---: | ---: | ---: | ----: |
| A000           | 10   | 20   | 30   | 60    |

As a data quality step, if a procedure has a total less than 100 (in the entirity of England in
2019/20) then we remove this procedure from our analysis.

We then classify the procedures based on the following:

* if a procedure has more than 50% of the procedures performed in outpatients, then classify it as
  "usually outpatients"
* if a procedure has more than 50% of the procedures performed as a daycase, then classify it as
  "usually daycase"
* if a procedure is not in either of the two "usually" groups, but more than 5% of the time it is
  performed in outpatients, then classify it as "occasionally outpatients"
* if a procedure is not in either of the two "usually" groups, but more than 5% of the time it is
  performed in daycase, then classify it as "occasionally daycase"

If a procedure does not match any of these groups then it is not classified.

For each of these groups, we perform a one-sided binomial test, and if we have a p-value greater
than 0.001 then we reject it from that group.

Note that while a procedure can only exist in one of the "usually" groups, and a procedure cannot
belong to a "usually" and an "occasionally" group, a procedure could belong to bother occasionally
groups.

## Modelling process

### Usually/Occasionally Daycase

Procedures that are flagged as usually/occasionally daycase, but are performed as an elective
inpatient admission are converted to be performed as a daycase, and the length of stay is reduced to
0.

### Usually/Occasionally Outpatients

Procedures that are flagged as usually/occasionally outpatients, but are performed as an elective
inpatient admission or daycase admission are converted to be performed in outpatients. These rows
are removed from the inpatients counts and are added to outpatients instead.
"""

import json

from pyspark.sql import functions as F

from hes_datasets import nhp_apc, primary_procedure
from mitigators import efficiency_mitigator


def _day_procedures(day_procedure_type):
    with open(
        "/Volumes/su_data/nhp/reference_data/day_procedures.json", "r", encoding="UTF-8"
    ) as f:
        codes = json.load(f)[day_procedure_type]

    classpats = ["1"] if day_procedure_type.endswith("dc") else ["1", "2"]

    return (
        nhp_apc.admission_has(primary_procedure, *codes)
        .filter(F.col("admimeth").startswith("1"))
        .filter(F.col("classpat").isin(classpats))
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator()
def _day_procedures_usually_dc():
    return _day_procedures("usually_dc")


@efficiency_mitigator()
def _day_procedures_usually_op():
    return _day_procedures("usually_op")


@efficiency_mitigator()
def _day_procedures_occasionally_dc():
    return _day_procedures("occasionally_dc")


@efficiency_mitigator()
def _day_procedures_occasionally_op():
    return _day_procedures("occasionally_op")
