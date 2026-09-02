import pyspark.sql.functions as F
from pyspark.sql.column import Column

from nhp.data.functional_areas.classifications.ae import *
from nhp.data.functional_areas.classifications.ip import *
from nhp.data.functional_areas.classifications.op import *


def class_age_adult() -> Column:
    return F.col("age") >= 18


def class_age_child() -> Column:
    return F.col("age") < 18


def class_medical() -> Column:
    return F.col("tretspef_type") == "Medical/Other"


def class_surgical() -> Column:
    return F.col("tretspef_type") == "Surgical"


def class_has_procedure() -> Column:
    return F.col("has_procedure")


def class_renal() -> Column:
    return F.col("tretspef") == "361"


def class_haem_onc() -> Column:
    return F.col("tretspef").isin("253", "303", "260", "370", "800")


def class_cardiology() -> Column:
    return F.col("tretspef").isin(["320", "321"])


def class_int_radiology() -> Column:
    return F.col("tretspef").isin(["811", "280"])
