from nhp.data.table_names.table_names import TableNames

udal = TableNames(
    # --------------------------------------------------------------------------
    # hes tables
    # --------------------------------------------------------------------------
    hes_aae="udal_lake_mart.newhospitalprogramme.hes_aae",
    hes_aae_diagnoses="udal_lake_mart.newhospitalprogramme.hes_aae_diagnoses",
    hes_aae_investigations="udal_lake_mart.newhospitalprogramme.hes_aae_investigations",
    hes_aae_treatments="udal_lake_mart.newhospitalprogramme.hes_aae_treatments",
    # ---
    hes_apc="udal_lake_mart.newhospitalprogramme.hes_apc",
    hes_apc_procedures="udal_lake_mart.newhospitalprogramme.hes_apc_procedures",
    hes_apc_diagnoses="udal_lake_mart.newhospitalprogramme.hes_apc_diagnoses",
    # ---
    hes_ecds="udal_silver_resticted.mesh_ecds.ec_core",
    # ---
    hes_opa="udal_lake_mart.newhospitalprogramme.hes_opa",
    hes_opa_procedures="udal_lake_mart.newhospitalprogramme.hes_opa_procedures",
    hes_opa_diagnoses="udal_lake_mart.newhospitalprogramme.hes_opa_diagnoses",
    # --------------------------------------------------------------------------
    # population projections tables
    # --------------------------------------------------------------------------
    population_projections_save_path="/Volumes/udal_lake_mart/newhospitalprogramme/files/population_projections",
    population_projections_births="udal_lake_mart.newhospitalprogramme.population_projections_births",
    population_projections_demographics="udal_lake_mart.newhospitalprogramme.population_projections_demographics",
    # --------------------------------------------------------------------------
    # reference tables
    # --------------------------------------------------------------------------
    reference_ccg_to_icb="udal_lake_mart.newhospitalprogramme.reference_ccg_to_icb",
    reference_icb_catchments="udal_lake_mart.newhospitalprogramme.reference_icb_catchments",
    reference_icd10_codes="udal_lake_mart.newhospitalprogramme.reference_icd10_codes",
    reference_ods_trusts="udal_lake_mart.newhospitalprogramme.reference_ods_trusts",
    reference_population_by_imd_decile="udal_lake_mart.newhospitalprogramme.reference_population_by_imd_decile",
    reference_provider_main_icb="udal_lake_mart.newhospitalprogramme.reference_provider_main_icb",
    reference_tretspef_grouping="udal_lake_mart.newhospitalprogramme.reference_tretspef_grouping",
    reference_lsoa11_to_lsoa21="udal_lake_mart.newhospitalprogramme.reference_lsoa11_to_lsoa21",
    reference_lsoa21_to_lad23="udal_lake_mart.newhospitalprogramme.reference_lsoa21_to_lad23",
    reference_pop_by_lsoa21="udal_lake_mart.newhospitalprogramme.reference_pop_by_lsoa21",
    reference_provider_lad23_splits="udal_lake_mart.newhospitalprogramme.reference_provider_lad23_splits",
    reference_pop_by_provider="udal_lake_mart.newhospitalprogramme.reference_pop_by_provider",
    reference_pop_by_lad23="udal_lake_mart.newhospitalprogramme.reference_pop_by_lad23",
    reference_lsoa11_to_lad23="udal_lake_mart.newhospitalprogramme.reference_lsoa11_to_lad23",
    reference_lad22_to_lad23="udal_lake_mart.newhospitalprogramme.reference_lad22_to_lad23",
    # ---
    reference_day_procedures_code_list="/Volumes/udal_lake_mart/newhospitalprogramme/files/reference/day_procedures.json",
    reference_frailty_risk_scores="/Volumes/udal_lake_mart/newhospitalprogramme/files/reference/frailty_risk_scores.csv",
    reference_lsoa11_to_imd19="udal_lake_mart.newhospitalprogramme.reference_lsoa11_to_imd19",
    reference_pop_by_lsoa="/Volumes/udal_lake_mart/newhospitalprogramme/files/reference/sape22_pop_by_lsoa.csv",
    reference_trust_types="/Volumes/udal_lake_mart/newhospitalprogramme/files/reference/trust_types.parquet",
    # --------------------------------------------------------------------------
    # raw data
    # --------------------------------------------------------------------------
    raw_data_apc="udal_lake_mart.newhospitalprogramme.raw_data_apc",
    raw_data_apc_mitigators="udal_lake_mart.newhospitalprogramme.raw_data_apc_mitigators",
    raw_data_ecds="udal_lake_mart.newhospitalprogramme.raw_data_ecds",
    raw_data_opa="udal_lake_mart.newhospitalprogramme.raw_data_opa",
    raw_data_opa_mitigators="udal_lake_mart.newhospitalprogramme.raw_data_opa_mitigators",
    # --------------------------------------------------------------------------
    # aggregated data
    # --------------------------------------------------------------------------
    aggregated_data_ecds="udal_lake_mart.newhospitalprogramme.aggregated_data_ecds",
    aggregated_data_opa="udal_lake_mart.newhospitalprogramme.aggregated_data_opa",
    # --------------------------------------------------------------------------
    # inputs data
    # --------------------------------------------------------------------------
    inputs_catchments="udal_lake_mart.newhospitalprogramme.reference_inputs_catchments",
    inputs_save_path="/Volumes/udal_lake_mart/newhospitalprogramme/files/inputs_data/dev",
    # --------------------------------------------------------------------------
    # default data
    # --------------------------------------------------------------------------
    default_apc="udal_lake_mart.newhospitalprogramme.default_apc",
    default_apc_mitigators="udal_lake_mart.newhospitalprogramme.default_apc_mitigators",
    default_ecds="udal_lake_mart.newhospitalprogramme.default_ecds",
    default_opa="udal_lake_mart.newhospitalprogramme.default_opa",
    default_hsa_activity_tables_provider="udal_lake_mart.newhospitalprogramme.default_hsa_activity_tables",
    default_hsa_activity_tables_icb="udal_lake_mart.newhospitalprogramme.default_hsa_activity_tables_icb",
    default_hsa_activity_tables_national="udal_lake_mart.newhospitalprogramme.default_hsa_activity_tables_national",
    default_inequalities="udal_lake_mart.newhospitalprogramme.default_inequalities",
    # --------------------------------------------------------------------------
    # model data
    # --------------------------------------------------------------------------
    model_data_path="/Volumes/udal_lake_mart/newhospitalprogramme/files/model_data",
    # --------------------------------------------------------------------------
)
