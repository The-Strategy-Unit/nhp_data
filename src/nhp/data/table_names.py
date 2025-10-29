from dataclasses import dataclass

# TODO:
environment = "mlcsu"  # mlcsu / udal


@dataclass
class TableNames:
    # --------------------------------------------------------------------------
    # hes tables
    # --------------------------------------------------------------------------
    hes_aae: str
    hes_aae_diagnoses: str
    hes_aae_investigations: str
    hes_aae_treatments: str
    # ---
    hes_apc: str
    hes_apc_procedures: str
    hes_apc_diagnoses: str
    # ---
    hes_ecds: str
    # ---
    hes_opa: str
    hes_opa_procedures: str
    hes_opa_diagnoses: str
    # --------------------------------------------------------------------------
    # population projections tables
    # --------------------------------------------------------------------------
    population_projections_births: str
    population_projections_demographics: str
    population_projections_custom: str
    # --------------------------------------------------------------------------
    # reference tables
    # --------------------------------------------------------------------------
    reference_ccg_to_icb: str
    reference_icb_catchments: str
    reference_icd10_codes: str
    reference_ods_trusts: str
    reference_population_by_imd_decile: str
    reference_provider_catchments: str
    reference_provider_main_icb: str
    reference_tretspef_grouping: str
    # TODO: convert these to tables
    reference_day_procedures_code_list: str
    reference_frailty_risk_scores: str
    reference_lad11_to_lad23_lookup: str
    reference_lsoa11_to_imd19: str
    reference_pop_by_lsoa: str
    reference_population_by_year: str
    reference_trust_types: str
    # --------------------------------------------------------------------------
    # raw data
    # --------------------------------------------------------------------------
    raw_data_apc: str
    raw_data_apc_mitigators: str
    raw_data_ecds: str
    raw_data_opa: str
    raw_data_opa_mitigators: str
    # --------------------------------------------------------------------------
    # aggregated data
    # --------------------------------------------------------------------------
    aggregated_data_ecds: str
    aggregated_data_opa: str
    # --------------------------------------------------------------------------
    # inputs data
    # --------------------------------------------------------------------------
    inputs_catchments: str
    inputs_save_path: str
    # --------------------------------------------------------------------------
    # default data
    # --------------------------------------------------------------------------
    default_apc: str
    default_apc_mitigators: str
    default_ecds: str
    default_opa: str
    default_hsa_activity_tables_provider: str
    default_hsa_activity_tables_icb: str
    default_hsa_activity_tables_national: str
    default_inequalities: str
    # --------------------------------------------------------------------------
    # model data
    # --------------------------------------------------------------------------
    model_data_path: str
    # --------------------------------------------------------------------------


mlcsu = TableNames(
    # --------------------------------------------------------------------------
    # hes tables
    # --------------------------------------------------------------------------
    hes_aae="hes.silver.aae",
    hes_aae_diagnoses="hes.silver.aae_diagnoses",
    hes_aae_investigations="hes.silver.aae_investigations",
    hes_aae_treatments="hes.silver.aae_treatments",
    # ---
    hes_apc="hes.silver.apc",
    hes_apc_procedures="hes.silver.apc_procedures",
    hes_apc_diagnoses="hes.silver.apc_diagnoses",
    # ---
    hes_ecds="hes.silver.ecds",
    # ---
    hes_opa="hes.silver.opa",
    hes_opa_procedures="hes.silver.opa_procedures",
    hes_opa_diagnoses="hes.silver.opa_diagnoses",
    # --------------------------------------------------------------------------
    # population projections tables
    # --------------------------------------------------------------------------
    population_projections_births="nhp.population_projections.births",
    population_projections_demographics="nhp.population_projections.demographics",
    population_projections_custom="/Volumes/nhp/population_projections/files",
    # --------------------------------------------------------------------------
    # reference tables
    # --------------------------------------------------------------------------
    reference_ccg_to_icb="strategyunit.reference.ccg_to_icb",
    reference_icb_catchments="nhp.reference.icb_catchments",
    reference_icd10_codes="strategyunit.reference.icd10_codes",
    reference_ods_trusts="nhp.reference.ods_trusts",
    reference_population_by_imd_decile="nhp.reference.population_by_imd_decile",
    reference_provider_catchments="nhp.reference.provider_catchments",
    reference_provider_main_icb="nhp.reference.provider_main_icb",
    reference_tretspef_grouping="nhp.reference.tretspef_grouping",
    # ---
    reference_day_procedures_code_list="/Volumes/nhp/reference/files/day_procedures.json",
    reference_frailty_risk_scores="/Volumes/nhp/reference/files/frailty_risk_scores.csv",
    reference_lad11_to_lad23_lookup="/Volumes/nhp/reference/files/lad11_to_lad23_lookup.csv",
    reference_lsoa11_to_imd19="strategyunit.reference.lsoa11_to_imd19",
    reference_pop_by_lsoa="/Volumes/strategyunit/reference/files/sape22_pop_by_lsoa.csv",
    reference_population_by_year="/Volumes/nhp/reference/files/population_by_year.parquet",
    reference_trust_types="/Volumes/nhp/reference/files/trust_types.parquet",
    # --------------------------------------------------------------------------
    # raw data
    # --------------------------------------------------------------------------
    raw_data_apc="nhp.raw_data.apc",
    raw_data_apc_mitigators="nhp.raw_data.apc_mitigators",
    raw_data_ecds="nhp.raw_data.ecds",
    raw_data_opa="nhp.raw_data.opa",
    raw_data_opa_mitigators="nhp.raw_data.opa_mitigators",
    # --------------------------------------------------------------------------
    # aggregated data
    # --------------------------------------------------------------------------
    aggregated_data_ecds="nhp.aggregated_data.ecds",
    aggregated_data_opa="nhp.aggregated_data.opa",
    # --------------------------------------------------------------------------
    # inputs data
    # --------------------------------------------------------------------------
    inputs_catchments="nhp.reference.inputs_catchments",
    inputs_save_path="/Volumes/nhp/inputs_data/files/dev",
    # --------------------------------------------------------------------------
    # default data
    # --------------------------------------------------------------------------
    default_apc="nhp.default.apc",
    default_apc_mitigators="nhp.default.apc_mitigators",
    default_ecds="nhp.default.ecds",
    default_opa="nhp.default.opa",
    default_hsa_activity_tables_provider="nhp.default.hsa_activity_tables",
    default_hsa_activity_tables_icb="nhp.default.hsa_activity_tables_icb",
    default_hsa_activity_tables_national="nhp.default.hsa_activity_tables_national",
    default_inequalities="nhp.default.inequalities",
    # --------------------------------------------------------------------------
    # model data
    # --------------------------------------------------------------------------
    model_data_path="/Volumes/nhp/model_data/files",
    # --------------------------------------------------------------------------
)

table_names: TableNames
match environment:
    case "mlcsu":
        table_names = mlcsu
    case "udal":
        raise NotImplementedError(
            "Table names for UDAL environment are not yet defined."
        )
    case _:
        raise ValueError(f"Unknown environment: {environment}")
