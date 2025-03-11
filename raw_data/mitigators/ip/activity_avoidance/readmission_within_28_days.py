"""Readmissions Within 28 Days of Discharge (IP-AA-028)

Emergency readmission to hospital is typically regarded as an unfavourable outcome. 
There are a range of interventions that have been shown to be effective in reducing the level of
readmissions, including patient education, timely outpatient appointments, medication
reconciliation, and telephone follow ups.
The model identifies patients who are readmitted within 28 days of being discharged from hospital. 

Some of these patients may have been discharged from a different hospital than the one they were
readmitted to.
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from hes_datasets import nhp_apc
from raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator("readmission_within_28_days")
def _readmission_within_28_days():
    # join the apc dataset to itself, such that:
    # 1. the person id matches on both sides
    # 2. the epikeys are not the same (not the same episode)
    # 3. the prior admission date is before the current admission date
    # 4. the prior discharge date is before the current discharge date
    # 5. the prior discharge date is on or before the current discharge date
    # 6. the difference between the prior discharge date and the current admission date is <= 28days
    #
    # this approach has one limitation: if the subsequent admission is a 0 day length of stay
    # admission, then by condition 4. will fail. without this condition, if you have two 0 day
    # admissions on the same day then both would be flagged as a readmission.
    #
    # it's possible that condition 2. could be relaxed to > from !=, but this may cause the logic to
    # fail across years if the epikeys are not unique across years
    readm = nhp_apc.alias("readm")

    # make sure to use full hes table - our nhp views filter on certain columns
    # (e.g. not all providers included)
    spark = DatabricksSession.builder.getOrCreate()
    prior = (
        spark.read.table("hes.silver.apc")
        .filter(F.col("last_episode_in_spell"))
        # remove well babies
        .filter(F.col("well_baby_ind") == "N")
        .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())
        .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
        #
        .withColumnRenamed("person_id_deid", "person_id")
        .alias("prior")
    )

    join_condition = [
        F.col("readm.person_id") == F.col("prior.person_id"),
        F.col("readm.epikey") != F.col("prior.epikey"),
        (F.col("readm.admidate") > F.col("prior.admidate"))
        | (F.col("readm.disdate") > F.col("prior.disdate")),
        F.datediff(F.col("readm.admidate"), F.col("prior.disdate")).between(0, 28),
    ]

    return (
        readm.filter(F.col("admimeth").rlike("^2"))
        .join(prior, join_condition, "semi")
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
