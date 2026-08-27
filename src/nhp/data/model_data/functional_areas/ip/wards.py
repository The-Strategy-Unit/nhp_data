import pyspark.sql.functions as F
from nhp.capacity.functional_areas.ip_daycase import *
from nhp.capacity.functional_areas.ip_maternity import *
from nhp.capacity.functional_areas.ip_wards import *
from nhp.capacity.functional_areas.processing_helpers import *
from pyspark.sql import DataFrame, SparkSession, Window

from nhp.data.table_names import table_names


def get_ip_functional_area_wards(apc: DataFrame, spark: SparkSession) -> DataFrame:
    episode_primary_procedure = (
        spark.read.table(table_names.hes_apc_procedures)
        .filter(F.col("procedure_order") == 1)
        .select(
            "epikey",
            "fyear",
            "procode3",
            F.col("procedure_code").alias("primary_procedure"),
        )
    )

    episodes = (
        spark.read.table(table_names.hes_apc)
        .filter(F.col("FCE") == 1)
        .join(episode_primary_procedure, ["epikey", "fyear", "procode3"], "left")
        .withColumn("has_procedure", ~F.col("primary_procedure").isNull())
        .drop("fyear", "speldur")
    )

    df = (
        apc.select(
            "rn",
            "susspellid",
            "dataset",
            # apply last episode values to the rest of the episodes
            "fyear",
            "group",
            "age",
        )
        .join(episodes, "susspellid")
        .withColumn("is_zero_length_episode", (F.col("epidur") == 0).cast("int"))
        .join(spark.read.table(table_names.reference_tretspef_type), "tretspef", "left")
        # TODO: this should be in the functional area package
        .withColumn(
            "grouping",
            F
            # maternity
            .when(is_normal_delivery_zerolos(), "maternity_normal_delivery_zerolos")
            .when(
                is_normal_delivery_nonzerolos(), "maternity_normal_delivery_nonzerolos"
            )
            .when(is_assisted_delivery_zerolos(), "maternity_assisted_delivery_zerolos")
            .when(
                is_assisted_delivery_nonzerolos(),
                "maternity_assisted_delivery_nonzerolos",
            )
            .when(is_maternity_assessment(), "maternity_assessment")
            .when(
                is_nonelective_csection_zerolos(),
                "maternity_nonelective_csection_zerolos",
            )
            .when(
                is_nonelective_csection_nonzerolos(),
                "maternity_nonelective_csection_nonzerolos",
            )
            .when(is_elective_csection_zerolos(), "maternity_elective_csection_zerolos")
            .when(
                is_elective_csection_nonzerolos(),
                "maternity_elective_csection_nonzerolos",
            )
            .when(is_overnight_no_birth_event(), "maternity_overnight_no_birth")
            # daycase
            .when(
                is_renal_elective() | is_renal_regular_day_night(),
                "daycase_renal_episode",
            )
            .when(is_daycase_haem_onc(), "daycase_haem_onc_episode")
            .when(is_daycase_endoscopy(), "daycase_endoscopy_episode")
            .when(is_daycase_adult_medical(), "daycase_adult_medical_episode")
            .when(is_daycase_adult_surgical(), "daycase_adult_surgical_episode")
            .when(is_daycase_child_medical(), "daycase_child_medical_episode")
            .when(is_daycase_child_surgical(), "daycase_child_surgical_episode")
            # ip
            .otherwise(build_ward_grouping_column()),
        )
    )

    w = Window.partitionBy("rn")
    return (
        df.groupBy("fyear", "dataset", "rn", "sitetret", "grouping")
        .agg(
            F.sum("epidur").alias("group_los"),
            F.count("epikey").alias("episodes"),
            F.sum("is_zero_length_episode").alias("zero_length_episodes"),
        )
        .withColumn("los_total", F.sum("group_los").over(w))
        .withColumn(
            "group_pcnt",
            F.when(F.col("los_total") == 0, 0).otherwise(
                F.col("group_los") / F.col("los_total")
            ),
        )
    )
