from nhp.data.table_names.table_names import TableNames

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
    population_projections_save_path="/Volumes/nhp/population_projections/files",
    population_projections_births="nhp.population_projections.births",
    population_projections_demographics="nhp.population_projections.demographics",
    # --------------------------------------------------------------------------
    # reference tables
    # --------------------------------------------------------------------------
    reference_ccg_to_icb="strategyunit.reference.ccg_to_icb",
    reference_icb_catchments="nhp.reference.icb_catchments",
    reference_icd10_codes="strategyunit.reference.icd10_codes",
    reference_ods_trusts="nhp.reference.ods_trusts",
    reference_population_by_imd_decile="nhp.reference.population_by_imd_decile",
    reference_provider_main_icb="nhp.reference.provider_main_icb",
    reference_tretspef_grouping="nhp.reference.tretspef_grouping",
    reference_lsoa11_to_lsoa21="nhp.reference.lsoa11_to_lsoa21",
    reference_lsoa21_to_lad23="nhp.reference.lsoa21_to_lad23",
    reference_pop_by_lsoa21="nhp.reference.pop_by_lsoa21",
    reference_provider_lad23_splits="nhp.reference.provider_lad23_splits",
    reference_pop_by_provider="nhp.reference.pop_by_provider",
    reference_pop_by_lad23="nhp.reference.pop_by_lad23",
    reference_lsoa11_to_lad23="nhp.reference.lsoa11_to_lad23",
    reference_lad22_to_lad23="nhp.reference.lad22_to_lad23",
    # ---
    reference_day_procedures_code_list="/Volumes/nhp/reference/files/day_procedures.json",
    reference_frailty_risk_scores="/Volumes/nhp/reference/files/frailty_risk_scores.csv",
    reference_lsoa11_to_imd19="strategyunit.reference.lsoa11_to_imd19",
    reference_pop_by_lsoa="/Volumes/strategyunit/reference/files/sape22_pop_by_lsoa.csv",
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
