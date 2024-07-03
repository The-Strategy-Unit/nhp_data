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

from pyspark.sql import functions as F

from hes_datasets import diagnoses, nhp_apc
from mitigators import efficiency_mitigator


def _ambulatory_emergency_care(conditions):
    return (
        nhp_apc.join(diagnoses, ["epikey", "fyear"])
        .filter(F.col("age") >= 18)
        .filter(F.col("admimeth").rlike("^2[123]"))
        .filter(F.col("diag_order") == 1)
        .filter(conditions)
        .select("epikey")
        # TODO: should we enable this?
        # .filter(F.col("speldur") > 0)
        .withColumn("sample_rate", F.lit(1.0))
    )


# helper functions for generating regexs:
# both r and s take an array (xs) of patterns to concatenate (|)
# r uses a fixed starting character of ^
def _aec_r(xs):
    return _aec_s("^", xs)


def _aec_s(x, xs):
    return f"{x}({'|'.join(xs)})"


# helper function for generating a hrg+diagnosis filter
def _aec_f(sushrg, diagnosis):
    s = F.col("sushrg").rlike(sushrg)
    d = F.col("diagnosis").rlike(diagnosis)
    return s & d


@efficiency_mitigator()
def _ambulatory_emergency_care_low():
    r, s, f = _aec_r, _aec_s, _aec_f

    return _ambulatory_emergency_care(
        f(r(["DZ26[NP]"]), r(["J93[0189]"]))
        | f(r(["DZ15[PQR]"]), r(["J45[0189]"]))
        | f(
            r(["DZ65[FGHJK]"]),
            s("^J", ["21[0189]", s("4", ["[02]X", "10", "3[1289]", "4[0189]"])]),
        )
        | f(
            s("^DZ", ["11[TUV]", "23[MN]"]),
            s(
                "^J1",
                ["[01]0", "2[12389]", "[34]X", "5[3-9]", "6[08]", "7[018]", "8[0189]"],
            ),
        )
        | f(r(["FZ38[MNP]"]), s("^K", ["2(0X|1[09]|2[16]|2[5-8][046])", "92[012]"]))
    )


@efficiency_mitigator()
def _ambulatory_emergency_care_moderate():
    r, s, f = _aec_r, _aec_s, _aec_f

    return _ambulatory_emergency_care(
        f(r(["EB03[DE]"]), s("^I", ["1(10|3[02])", "50[019]"]))
        | f(
            r(["EB07[BCDE]"]),
            r(
                [
                    s(
                        "I14",
                        ["4[014567]", "5[0-9]", "7[19]", "8[012349]", "9[124589]"],
                    ),
                    "R00[0128]",
                ]
            ),
        )
        | f(
            r(["EB1([04][CDE]|[23][ABC]|3D)", "DZ28[AB]"]),
            r(
                [
                    "I2([045][189]|5[026])",
                    "M94[01]",
                    "R0(1[12]|7[234]|91)",
                    "Z03[45]",
                ]
            ),
        )
        | f(r(["AA31[CDE]"]), r(["G(4(3[012389]|4[01348])|971)", "R51X"]))
        | f(r(["LA04[QRS]"]), s("^N", ["1(1[0189]|36)", "3(0[0123489]|4[123]|90)"]))
        | f(r(["KC05[KLMN]"]), s("^E", ["222", "612", "8(3[45]|6X|7[015678])"]))
        | f(r("LA07[NP]"), s("^N", ["17[89]", "990"]))
        | f(
            s("^HD2", ["3[FDHJ]", "6[DEFG]"]),
            s(
                "^M",
                [
                    "0([56][089]|6[123])",
                    "1(0|1[09]|3)",
                    "2(0[0-389]|5[569])",
                    "6(6[01]|73)",
                    "712",
                ],
            ),
        )
        | f(
            r(["GC18[AB]"]),
            r(
                [
                    "C2(3X|4[0189]|5[0139])",
                    "D(135|376)",
                    s("K8", ["05", "2[123489]", "3[19]", "70"]),
                    "R17X",
                ]
            ),
        )
        | f(
            r(["FZ90B"]),
            r(
                [
                    "I880",
                    s("K", ["2(55|97)", "56([346]|9[0189])", "913"]),
                    "N(832|940)",
                    s("R1", ["0[1234]", "1X", "9[01458]"]),
                ]
            ),
        )
        | f(s("^FZ", ["91[KLM]", "20[HJ]"]), s("^K", ["35[38]", "37X", "500"]))
        | f(r(["FZ91[KLM]", "MB09[EF]"]), r(["K5(62|7[1359])", "N739"]))
        | f(r(["NZ(1[89]|20)[AB]"]), r(["O21[01289]"]))
    )


@efficiency_mitigator()
def _ambulatory_emergency_care_high():
    r, s, f = _aec_r, _aec_s, _aec_f

    return _ambulatory_emergency_care(
        f(r(["DZ(09[NPQ]|28[AB])"]), r(["I26[09]", "R0[79]"]))
        | f(r(["DZ16[QR]"]), r(["C782", "J9([01]|4[08])"]))
        | f(r(["DZ22[PQ]"]), r(["J2(0|22X)"]))
        | f(
            r(["DZ(19[LMN]|2(5[KLM]|7[TU]))"]),
            r(
                [
                    "E622",
                    s("J", ["8(0X|4[0189])", "9(6[019]|8[014689]|98)"]),
                    "Q340",
                    s("R0", ["4[289]", "5X", "6[024]", "98"]),
                ]
            ),
        )
        | f(r(["AA29[CDEF]"]), r(["G45[0123489]"]))
        | f(r(["AA26[EFGH]"]), r(["R568"]))
        | f(r(["AA26[EFGH]"]), r(["G(253|40)", "R568"]))
        | f(
            r(["FZ36[NPQ]"]),
            r(
                [
                    s(
                        "A0",
                        [
                            "2[0289]",
                            "4[45689]",
                            "5[489]",
                            "7[12]",
                            "8[0-5]",
                            "9[09]",
                        ],
                    ),
                    "K52[01289]",
                ]
            ),
        )
        | f(r(["FZ37[QRS]"]), r(["K5(0[0189]|1[023459]|23)"]))
        | f(
            r(["GC(01F|1[27][JK])"]),
            r(
                [
                    s("C", ["2(2[012479]|3[X3479]|4[0189]|5[^56])", "787"]),
                    "D(135|376)",
                    s(
                        "K",
                        [
                            "7(0[012349]|2[019]|3[01289]|4[0123456]|5[23489]|6[01689])",
                            "8(0[0123458]|1[0189]|2[123489]|3[1489]|6[012389]|70)",
                            "915",
                        ],
                    ),
                    "R(1(6[012]|7X)|945)",
                ]
            ),
        )
        | f(
            s("^SA0", ["1[HJK]", "3[H]", "4[HJKL]", "5[HJ]", "6[HJK]", "9[HJKL]"]),
            s(
                "^D",
                [
                    "46[012479]",
                    s(
                        "5",
                        [
                            "0[0189]",
                            "1[0-389]",
                            "2[0189]",
                            "[37]1",
                            "8[01289]",
                            "9[012489]",
                        ],
                    ),
                    "D64[89]",
                ],
            ),
        )
        | f(
            s("KB0", ["1[CDEF]", "2[HJK]", "3[DE]"]),
            s("^E1", ["[01]", "[34][2-9]", "6[012]"]),
        )
        | f(r(["JD07[HJK]"]), r(["I891", "L0(10|3[0-389]|8[089])"]))
        | f(
            s("^FZ9", ["1[KLM]", "2[JK]"]),
            r(["C15[0-589]", "K2(2[0245789]|38)", "R1[23]X", "T181"]),
        )
        | f(
            r(["EB08[ABCDE]", s("WH", ["16[AB]", "09[EFG]"])]),
            r(["I951", s("R", ["2(68|96)", "5[45]X"]), "T671"]),
        )
        | f(r(["FZ13C"]), r(["R18X"]))
        | f(
            s(
                "^HE",
                [
                    "2(1[FG]|22[GHJK])",
                    "3(1[EFG]|2[CDE])",
                    "4(1[BCD]|2[CDE])",
                    "5(1[EFGH]|2[CDEF])",
                ],
            ),
            s(
                "^S",
                [
                    "4(2[023489]|3[0123])",
                    "5(2[0-9]|3[0123])",
                    "6(2[0-8]|3[02]|7[08])",
                    "72[4]",
                    "8(2[0-9]|3[0123])",
                    "9(2[0-579]|3[01]|71|290)",
                ],
            ),
        )
        | f(s("HE1", ["1[GH]", "2[CDE]"]), r(["M2555", "S760"]))
        | f(r(["FZ38[MNP]"]), r(["K625", "K922"]))
        | f(
            r(
                [
                    s("FZ", ["91[KLM]", "2(2[DE]|3A|1D)"]),
                    s("JA", ["13[BC]", "4[45]Z"]),
                ]
            ),
            r(
                [
                    s("K6", ["1[0234]", "20"]),
                    s("L", ["0(2[0123489]|5[09])", "72[01]"]),
                    "N61X",
                ]
            ),
        )
        | f(r(["AA26[EFGH]", "CB02[DEF]"]), s("^S0", ["60", "9[89]"]))
        | f(
            r(["FZ91[KLM]"]),
            s(
                "^K",
                [
                    "563",
                    s("8", ["0[0-48]", "1[0189]", "[23][089]", "24", "31", "70"]),
                ],
            ),
        )
        | f(
            s("^FZ", ["91[KLM]", "18[JK]"]),
            s("^K4", ["[01][29]", "29", "3[259]", "58", "69"]),
        )
        | f(s("^FZ2", ["2[DE]", "3A", "1D"]), r(["K(594|60[01235])", "T185"]))
        | f(
            s("^LB", ["16[GHJK]", "28[EFG]"]),
            r([s("N", ["3(20|9[134])", "4(0X|2[89])"]), "R3[23]X"]),
        )
        | f(r(["LB40[EFG]"]), s("^N2", ["0[0129]", "1[0189]", "3X"]))
        | f(s("^L", ["A09[PQ]", "B3(7[CDE]|8[FGH])"]), s("^R3", ["0[09]", "1X"]))
        | f(s("^LB", ["1(5E|8Z)", "20[EFG]"]), r(["T83[0135689]"]))
        | f(
            s("^LB", ["35[EFGH]", "54A"]),
            s("^N", ["4(3[01234]|4X|5[09X]|9[01289])", "508"]),
        )
    )


@efficiency_mitigator()
def _ambulatory_emergency_care_very_high():
    r, s, f = _aec_r, _aec_s, _aec_f

    return _ambulatory_emergency_care(
        f(r(["YQ51[CDE]"]), r(["I8(0[123]|22)", "M79[68]"]))
        | f(r(["FZ91[KLM]"]), r(["T85[58]", "Z431"]))
        | f(r(["HC27[LMN]", "HD39[GH]"]), s("^M8", ["0[^67]", "4[34]"]))
        | f(s("^HE1", ["1[GH]", "2[CDE]"]), r(["S325"]))
        | f(s("^FZ2", ["1D", "2[DE]", "3A"]), r(["K64[12389]", "O(224|872)"]))
        | f(
            r(["MB08B"]),
            s("^O", [s("0", ["2[0189]", "3[1-9]", "[56][49]"]), "20[089]"]),
        )
        | f(r(["MA2[23]Z", "MB09[DEF]"]), r(["N75[0189]"]))
    )
