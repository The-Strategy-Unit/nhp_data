from dataclasses import dataclass


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
    population_projections_save_path: str
    population_projections_births: str
    population_projections_demographics: str
    # --------------------------------------------------------------------------
    # reference tables
    # --------------------------------------------------------------------------
    reference_ccg_to_icb: str
    reference_icb_catchments: str
    reference_icd10_codes: str
    reference_ods_trusts: str
    reference_population_by_imd_decile: str
    reference_provider_main_icb: str
    reference_tretspef_grouping: str
    reference_lsoa11_to_lsoa21: str
    reference_lsoa21_to_lad23: str
    reference_lsoa11_to_lad23: str
    reference_pop_by_lsoa21: str
    reference_provider_lad23_splits: str
    reference_pop_by_provider: str
    reference_pop_by_lad23: str
    reference_lad22_to_lad23: str
    # TODO: convert these to tables
    reference_day_procedures_code_list: str
    reference_frailty_risk_scores: str
    reference_lsoa11_to_imd19: str
    reference_pop_by_lsoa: str
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
