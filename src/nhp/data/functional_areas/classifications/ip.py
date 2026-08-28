import pyspark.sql.functions as F
from pyspark.sql.column import Column

ENDOSCOPY_PROCEDURE_CODES = [
    "G14",
    "G15",
    "G16",
    "G17",
    "G18",
    "G19",
    "G20",
    "G42",
    "G43",
    "G44",
    "G45",
    "G46",
    "G54",
    "G55",
    "G64",
    "G65",
    "G79",
    "G80",
    "H23",
    "H24",
    "H25",
    "H26",
    "H27",
    "H28",
    "H37",
    "H69",
    "H70",
    "H71",
    "J38",
    "J39",
    "J40",
    "J41",
    "J42",
    "J43",
    "J44",
    "J45",
    "H20",
    "H21",
    "H22",
    "H68",
    "E48",
    "E49",
    "E50",
    "E51",
]

ASSISTED_BIRTHS_CODES = ["R19", "R20", "R21", "R22", "R23"]

CARDIAC_PROCEDURE_CODES = ["K63", "K75", "K60", "K62", "K59", "K57", "K73", "K49"]


def class_non_elective() -> Column:
    return (
        (F.col("admimeth").startswith("2"))
        & (F.col("classpat") == "1")
        & (F.col("group") != "maternity")
    )


def class_elective() -> Column:
    return (F.col("admimeth").startswith("1")) & (F.col("classpat") == "1")


def class_zero_los() -> Column:
    return F.col("speldur") == 0


def class_non_zero_los() -> Column:
    return F.col("speldur") > 0


def class_regular_day_night() -> Column:
    return F.col("classpat").isin("3", "4")


def class_daycase() -> Column:
    return (F.col("admimeth").isin("11", "12", "13")) & (F.col("classpat") == "2")


def class_endoscopy() -> Column:
    return F.col("primary_procedure").substr(1, 3).isin(ENDOSCOPY_PROCEDURE_CODES)


def class_maternity() -> Column:
    return F.col("group") == "maternity"


def class_birth_event() -> Column:
    return F.col("maternity_episode_type") == 1


def class_birth_normal() -> Column:
    return F.col("primary_procedure").substr(1, 3) == "R24"


def class_birth_assisted() -> Column:
    return F.col("primary_procedure").substr(1, 3).isin(ASSISTED_BIRTHS_CODES)


def class_birth_nonelective_c_section() -> Column:
    return F.col("primary_procedure").substr(1, 3) == "R18"


def class_no_birth_event() -> Column:
    return ~class_birth_event()


def class_birth_elective_csection() -> Column:
    return F.col("primary_procedure").substr(1, 3) == "R17"


def class_cardiac_cath() -> Column:
    return F.col("primary_procedure").substr(1, 3).isin(CARDIAC_PROCEDURE_CODES)
