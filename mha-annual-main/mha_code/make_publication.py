#%%
import write_excel
from helpers import get_config, get_excel_template_dir, configure_logging
from pathlib import Path
import timeit
from datetime import datetime
import logging
import pandas as pd
from demog_params import lower_age, upper_age, all_age, all_lower_age, upper_eth, upper_eth_desc, gen, gen_desc, gen_desc_all, gen_desc_unk, white_eth, white_eth_short, white_eth_desc, mixed_eth, asian_eth, black_eth, other_eth, other_eth_desc, unknown_eth, imd, lod_age, lod_upper_eth, lod_lower_eth

def main():
    config = get_config()

    year = config['year']  
    data_dir = Path(config['data_dir'])  
    output_dir = Path(config['output_dir'])
    log_dir = Path(config['log_dir'])
    template_dir = get_excel_template_dir()

    configure_logging(log_dir)
    logger = logging.getLogger(__name__)
    logger.info(f"Logging the config settings:\n\n\t{config}\n")
    logger.info(f"Starting run at:\t{datetime.now().time()}")

    #### CSV and Excel production ####

    # MHA CSVs
    csv_main_path = data_dir / f"mha_annual_suppressed_{year}.csv"
    csv_lod_path = data_dir / f"mha_annual_lod_{year}.csv"
    csv_rate_path = data_dir / f"mha_annual_rates_{year}.csv"
    csv_episode_path = data_dir / f"mha_annual_episodes_{year}.csv"
    csv_1e_path = data_dir / f"mha_annual_table_1e_{year}.csv"
    csv_1h_path = data_dir / f"mha_annual_table_1h_{year}.csv"
    csv_dq_prov_subm_det_path = data_dir / f"mha_dq_prov_type_sub_det_{year}.csv"
    csv_dq_prov_comp_path = data_dir / f"mha_dq_prov_comp_12m_{year}.csv"
    csv_mhs08_time_series_path = data_dir / f"mha_mhs08_time_series_{year}.csv"
    csv_mhs08_prov_type_path = data_dir / f"mha_mhs08_prov_type_{year}.csv"
    mha_rates_df = pd.read_csv(csv_rate_path)
    mha_pop_df = write_excel.prepare_pop_data(csv_rate_path)    
        
    # To produce MHA Main Excel Tables
    main_excel_template = template_dir / 'ment-heal-act-stat-eng-main-template.xlsx'    
    main_excel_output = output_dir / f"ment_heal_act_stat_eng_main_{year}.xlsx"
    main_table1a_data = write_excel.prepare_table1a_data(csv_main_path)
    main_table1b_data = write_excel.prepare_age_gender_rates_data(mha_rates_df, "All detentions")
    main_table1c_data = write_excel.prepare_eth_rates_data(mha_rates_df, "All detentions")
    main_table1d_data = write_excel.prepare_stp_rates_data(mha_rates_df, "All detentions")
    main_table1e_data = pd.read_csv(csv_1e_path)
    sub_age_table1e_data = write_excel.prepare_crosstab_data(csv_1e_path, all_age)
    main_table1f_data = write_excel.prepare_ccg_rates_data(mha_rates_df, "All detentions")
    main_table1g_data = write_excel.prepare_imd_rates_data(mha_rates_df, "All detentions")
    main_table1h_data = pd.read_csv(csv_1h_path)
    main_table2a_data = write_excel.prepare_table2a_data(csv_main_path)
    main_table2b_data = write_excel.prepare_age_gender_rates_data(mha_rates_df, "Short term orders")
    main_table2c_data = write_excel.prepare_eth_rates_data(mha_rates_df, "Short term orders") 
    main_table3a_data = write_excel.prepare_table3a_data(csv_main_path)
    main_table3b_data = write_excel.prepare_age_gender_rates_data(mha_rates_df, "Uses of CTOs")
    main_table3c_data = write_excel.prepare_eth_rates_data(mha_rates_df, "Uses of CTOs") 
    main_table4_data = write_excel.prepare_table4_data(csv_main_path)
    mha_prov_df = write_excel.prepare_table5_provider_data(main_excel_template)
    main_table5_data = write_excel.prepare_table5_data(csv_main_path)
    main_table6_data = write_excel.prepare_table6_data(csv_main_path)
    main_table7a_data = write_excel.prepare_age_gender_rates_data(mha_rates_df, "Discharges following detention")
    main_table7b_data = write_excel.prepare_eth_rates_data(mha_rates_df, "Discharges following detention")
    main_table8a_data = write_excel.prepare_lod_data(csv_lod_path, "MHA only")
    main_table8b_data = write_excel.prepare_lod_data(csv_lod_path, "MHA and CTO")
    main_table8c_data = write_excel.prepare_episode_data(csv_episode_path)

    # MHA Main Table 1a tag data groups
    table_1a_1_data = write_excel.prepare_table_1a_1(main_table1a_data, "All submissions")    
    table_1a_2_data = write_excel.prepare_table_1a_2(main_table1a_data, "All submissions")
    table_1a_3_data = write_excel.prepare_table_1a_3(main_table1a_data, "All submissions")
    table_1a_4_data = write_excel.prepare_table_1a_4(main_table1a_data, "All submissions")
    table_1a_5_data = write_excel.prepare_table_1a_5(main_table1a_data, "All submissions")
    table_1a_6_data = write_excel.prepare_table_1a_6(main_table1a_data, "All submissions")
    table_1a_7_data = write_excel.prepare_table_1a_7(main_table1a_data, "All submissions")
    table_1a_8_data = write_excel.prepare_table_1a_8(main_table1a_data, "All submissions")
    table_1a_9_data = write_excel.prepare_table_1a_1(main_table1a_data, "NHS TRUST")    
    table_1a_10_data = write_excel.prepare_table_1a_2(main_table1a_data, "NHS TRUST")
    table_1a_11_data = write_excel.prepare_table_1a_3(main_table1a_data, "NHS TRUST")
    table_1a_12_data = write_excel.prepare_table_1a_4(main_table1a_data, "NHS TRUST")
    table_1a_13_data = write_excel.prepare_table_1a_5(main_table1a_data, "NHS TRUST")
    table_1a_14_data = write_excel.prepare_table_1a_6(main_table1a_data, "NHS TRUST")
    table_1a_15_data = write_excel.prepare_table_1a_7(main_table1a_data, "NHS TRUST")
    table_1a_16_data = write_excel.prepare_table_1a_8(main_table1a_data, "NHS TRUST")
    table_1a_17_data = write_excel.prepare_table_1a_1(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")    
    table_1a_18_data = write_excel.prepare_table_1a_2(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_19_data = write_excel.prepare_table_1a_3(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_20_data = write_excel.prepare_table_1a_4(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_21_data = write_excel.prepare_table_1a_5(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_22_data = write_excel.prepare_table_1a_6(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_23_data = write_excel.prepare_table_1a_7(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")
    table_1a_24_data = write_excel.prepare_table_1a_8(main_table1a_data, "INDEPENDENT HEALTH PROVIDER")

    # MHA Main Table 1b tag data groups
    table_1b_1_data = write_excel.prepare_table_age_gender_1(main_table1b_data)
    table_1b_2_data = write_excel.prepare_table_age_gender_2(main_table1b_data)
    table_1b_3_data = write_excel.prepare_table_age_all(main_table1b_data, lower_age)
    table_1b_4_data = write_excel.prepare_table_age_all(main_table1b_data, upper_age)
    table_1b_5_data = write_excel.prepare_table_gender_all(main_table1b_data, gen_desc_all)

    # MHA Main Table 1c tag data groups
    table_1c_1_data = write_excel.prepare_table_eth_1(main_table1c_data)
    table_1c_2_data = write_excel.prepare_table_eth_2(main_table1c_data)
    table_1c_3_data = write_excel.prepare_table_eth_lower(main_table1c_data, white_eth)
    table_1c_4_data = write_excel.prepare_table_eth_lower(main_table1c_data, mixed_eth)
    table_1c_5_data = write_excel.prepare_table_eth_lower(main_table1c_data, asian_eth)
    table_1c_6_data = write_excel.prepare_table_eth_lower(main_table1c_data, black_eth)
    table_1c_7_data = write_excel.prepare_table_eth_lower(main_table1c_data, other_eth)

    # MHA Main Table 1d tag data groups
    table_1d_1_data = write_excel.prepare_table_stp_1(main_table1d_data)
    table_1d_2_data = write_excel.prepare_table_stp_2(main_table1d_data)
    table_1d_3_data = write_excel.prepare_table_stp_3(main_table1d_data)

    # MHA Main Table 1e data groups
    table_1e_1_data = write_excel.table_1e_crosstab_total(sub_age_table1e_data, upper_eth, "Ethnic6", "Count")
    table_1e_2_data = write_excel.table_1e_crosstab_total(sub_age_table1e_data, gen_desc_unk, "Der_Gender", "Count")
    table_1e_3_data = write_excel.table_1e_crosstab(main_table1e_data, upper_eth, "Ethnic6", lower_age, "Age", "Count")
    table_1e_4_data = write_excel.table_1e_crosstab(main_table1e_data, gen_desc_unk, "Der_Gender", lower_age, "Age", "Count")
    table_1e_5_data = write_excel.table_1e_crosstab(main_table1e_data, upper_eth, "Ethnic6", upper_age, "Age", "Count")
    table_1e_6_data = write_excel.table_1e_crosstab(main_table1e_data, gen_desc_unk, "Der_Gender", upper_age, "Age", "Count")
    table_1e_7_data = write_excel.table_1e_crosstab(sub_age_table1e_data, upper_eth, "Ethnic6", gen_desc_unk, "Der_Gender", "Count")
    table_1e_8_data = write_excel.table_1e_crosstab_same(sub_age_table1e_data, gen_desc_unk, "Der_Gender", "Count")
    table_1e_9_data = write_excel.table_1e_crosstab_total(sub_age_table1e_data, upper_eth, "Ethnic6", "CrudeRate")
    table_1e_10_data = write_excel.table_1e_crosstab_total(sub_age_table1e_data, gen_desc_unk, "Der_Gender", "CrudeRate")
    table_1e_11_data = write_excel.table_1e_crosstab(main_table1e_data, upper_eth, "Ethnic6", lower_age, "Age", "CrudeRate")
    table_1e_12_data = write_excel.table_1e_crosstab(main_table1e_data, gen_desc_unk, "Der_Gender", lower_age, "Age", "CrudeRate")
    table_1e_13_data = write_excel.table_1e_crosstab(main_table1e_data, upper_eth, "Ethnic6", upper_age, "Age", "CrudeRate")
    table_1e_14_data = write_excel.table_1e_crosstab(main_table1e_data, gen_desc_unk, "Der_Gender", upper_age, "Age", "CrudeRate")
    table_1e_15_data = write_excel.table_1e_crosstab(sub_age_table1e_data, upper_eth, "Ethnic6", gen_desc_unk, "Der_Gender", "CrudeRate")
    table_1e_16_data = write_excel.table_1e_crosstab_same(sub_age_table1e_data, gen_desc_unk, "Der_Gender", "CrudeRate")

    # MHA Main Table 1f tag data groups
    table_1f_1_data = write_excel.prepare_table_ccg_1(main_table1f_data)
    table_1f_2_data = write_excel.prepare_table_ccg_2(main_table1f_data)
    table_1f_3_data = write_excel.prepare_table_ccg_3(main_table1f_data)

    # MHA Main Table 1g tag data groups
    table_1g_1_data = write_excel.prepare_table_1g_1(main_table1g_data)
    table_1g_2_data = write_excel.prepare_table_1g_2(main_table1g_data)
    table_1g_3_data = write_excel.prepare_table_1g_imd_upper(main_table1g_data, imd)

    # MHA Main Table 1h tag data groups
    table_1h_1_data, table_1h_6_data = write_excel.prepare_table_1h_eth_lower(main_table1h_data, white_eth_desc)
    table_1h_2_data, table_1h_7_data = write_excel.prepare_table_1h_eth_lower(main_table1h_data, mixed_eth)
    table_1h_3_data, table_1h_8_data = write_excel.prepare_table_1h_eth_lower(main_table1h_data, asian_eth)
    table_1h_4_data, table_1h_9_data = write_excel.prepare_table_1h_eth_lower(main_table1h_data, black_eth)
    table_1h_5_data, table_1h_10_data = write_excel.prepare_table_1h_eth_lower(main_table1h_data, other_eth_desc)

    # MHA Main Table 1i tag data groups
    table_1i_1_data = write_excel.prepare_stp_demog(csv_rate_path, "Gender", gen_desc)
    table_1i_2_data = write_excel.prepare_stp_demog(csv_rate_path, "Age", all_lower_age)
    table_1i_3_data = write_excel.combine_stp_demog_lower(csv_rate_path, "STP; Gender", "STP; Age", gen_desc_unk, all_lower_age)

    # MHA Main Table 1j tag data groups
    table_1j_1_data = write_excel.prepare_stp_demog(csv_rate_path, "Ethnicity", upper_eth_desc)
    table_1j_2_data = write_excel.prepare_stp_demog(csv_rate_path, "IMD", imd)
    table_1j_3_data = write_excel.combine_stp_demog_lower(csv_rate_path, "STP; Ethnicity", "STP; IMD", upper_eth, imd)

    # MHA Main Table 2a tag data groups
    table_2a_1_data = write_excel.prepare_table_2a_1(main_table2a_data, "All submissions")    
    table_2a_2_data = write_excel.prepare_table_2a_2(main_table2a_data, "All submissions")
    table_2a_3_data = write_excel.prepare_table_2a_3(main_table2a_data, "All submissions")
    table_2a_4_data = write_excel.prepare_table_2a_4(main_table2a_data, "All submissions")
    table_2a_5_data = write_excel.prepare_table_2a_1(main_table2a_data, "NHS TRUST")    
    table_2a_6_data = write_excel.prepare_table_2a_2(main_table2a_data, "NHS TRUST")
    table_2a_7_data = write_excel.prepare_table_2a_3(main_table2a_data, "NHS TRUST")
    table_2a_8_data = write_excel.prepare_table_2a_4(main_table2a_data, "NHS TRUST")
    table_2a_9_data = write_excel.prepare_table_2a_1(main_table2a_data, "INDEPENDENT HEALTH PROVIDER")    
    table_2a_10_data = write_excel.prepare_table_2a_2(main_table2a_data, "INDEPENDENT HEALTH PROVIDER")
    table_2a_11_data = write_excel.prepare_table_2a_3(main_table2a_data, "INDEPENDENT HEALTH PROVIDER")
    table_2a_12_data = write_excel.prepare_table_2a_4(main_table2a_data, "INDEPENDENT HEALTH PROVIDER")

    # MHA Main Table 2b tag data groups
    table_2b_1_data = write_excel.prepare_table_age_gender_1(main_table2b_data)
    table_2b_2_data = write_excel.prepare_table_age_gender_2(main_table2b_data)
    table_2b_3_data = write_excel.prepare_table_age_all(main_table2b_data, lower_age)
    table_2b_4_data = write_excel.prepare_table_age_all(main_table2b_data, upper_age)
    table_2b_5_data = write_excel.prepare_table_gender_all(main_table2b_data, gen_desc_all)

    # MHA Main Table 2c tag data groups
    table_2c_1_data = write_excel.prepare_table_eth_1(main_table2c_data)
    table_2c_2_data = write_excel.prepare_table_eth_2(main_table2c_data)
    table_2c_3_data = write_excel.prepare_table_eth_lower(main_table2c_data, white_eth)
    table_2c_4_data = write_excel.prepare_table_eth_lower(main_table2c_data, mixed_eth)
    table_2c_5_data = write_excel.prepare_table_eth_lower(main_table2c_data, asian_eth)
    table_2c_6_data = write_excel.prepare_table_eth_lower(main_table2c_data, black_eth)
    table_2c_7_data = write_excel.prepare_table_eth_lower(main_table2c_data, other_eth)

    # MHA Main Table 3a tag data groups
    table_3a_1_data = write_excel.prepare_table_3a_1(main_table3a_data, "All submissions")    
    table_3a_2_data = write_excel.prepare_table_3a_2(main_table3a_data, "All submissions")
    table_3a_3_data = write_excel.prepare_table_3a_3(main_table3a_data, "All submissions")
    table_3a_4_data = write_excel.prepare_table_3a_4(main_table3a_data, "All submissions")
    table_3a_5_data = write_excel.prepare_table_3a_1(main_table3a_data, "NHS TRUST")    
    table_3a_6_data = write_excel.prepare_table_3a_2(main_table3a_data, "NHS TRUST")
    table_3a_7_data = write_excel.prepare_table_3a_3(main_table3a_data, "NHS TRUST")
    table_3a_8_data = write_excel.prepare_table_3a_4(main_table3a_data, "NHS TRUST")
    table_3a_9_data = write_excel.prepare_table_3a_1(main_table3a_data, "INDEPENDENT HEALTH PROVIDER")    
    table_3a_10_data = write_excel.prepare_table_3a_2(main_table3a_data, "INDEPENDENT HEALTH PROVIDER")
    table_3a_11_data = write_excel.prepare_table_3a_3(main_table3a_data, "INDEPENDENT HEALTH PROVIDER")
    table_3a_12_data = write_excel.prepare_table_3a_4(main_table3a_data, "INDEPENDENT HEALTH PROVIDER")

    # MHA Main Table 3b tag data groups
    table_3b_1_data = write_excel.prepare_table_age_gender_1(main_table3b_data)
    table_3b_2_data = write_excel.prepare_table_age_gender_2(main_table3b_data)
    table_3b_3_data = write_excel.prepare_table_age_all(main_table3b_data, lower_age)
    table_3b_4_data = write_excel.prepare_table_age_all(main_table3b_data, upper_age)
    table_3b_5_data = write_excel.prepare_table_gender_all(main_table3b_data, gen_desc_all)

    # MHA Main Table 3c tag data groups
    table_3c_1_data = write_excel.prepare_table_eth_1(main_table3c_data)
    table_3c_2_data = write_excel.prepare_table_eth_2(main_table3c_data)
    table_3c_3_data = write_excel.prepare_table_eth_lower(main_table3c_data, white_eth)
    table_3c_4_data = write_excel.prepare_table_eth_lower(main_table3c_data, mixed_eth)
    table_3c_5_data = write_excel.prepare_table_eth_lower(main_table3c_data, asian_eth)
    table_3c_6_data = write_excel.prepare_table_eth_lower(main_table3c_data, black_eth)
    table_3c_7_data = write_excel.prepare_table_eth_lower(main_table3c_data, other_eth)

    # MHA Table 4 tag groups
    table_4_1_data = write_excel.prepare_table_4_1(main_table4_data, "All submissions")
    table_4_2_data = write_excel.prepare_table_4_2(main_table4_data, "All submissions")
    table_4_3_data = write_excel.prepare_table_4_1(main_table4_data, "NHS TRUST")
    table_4_4_data = write_excel.prepare_table_4_2(main_table4_data, "NHS TRUST")
    table_4_5_data = write_excel.prepare_table_4_1(main_table4_data, "INDEPENDENT HEALTH PROVIDER")
    table_4_6_data = write_excel.prepare_table_4_2(main_table4_data, "INDEPENDENT HEALTH PROVIDER")

    # MHA Table 5 tag groups
    table_5_1_data = write_excel.prepare_table5_all_prov(main_table5_data, "People subject to the Act on 31st March")
    table_5_2_data = write_excel.prepare_table5_prov_type(main_table5_data, "People subject to the Act on 31st March")
    table_5_3_data = write_excel.prepare_table5_providers(main_table5_data, "People subject to the Act on 31st March", mha_prov_df)
    table_5_4_data = write_excel.prepare_table5_all_prov(main_table5_data, "People detained in hospital on 31st March")
    table_5_5_data = write_excel.prepare_table5_prov_type(main_table5_data, "People detained in hospital on 31st March")
    table_5_6_data = write_excel.prepare_table5_providers(main_table5_data, "People detained in hospital on 31st March", mha_prov_df)
    table_5_7_data = write_excel.prepare_table5_all_prov(main_table5_data, "People subject to Community Treatment Orders (CTOs) on 31st March")
    table_5_8_data = write_excel.prepare_table5_prov_type(main_table5_data, "People subject to Community Treatment Orders (CTOs) on 31st March")
    table_5_9_data = write_excel.prepare_table5_providers(main_table5_data, "People subject to Community Treatment Orders (CTOs) on 31st March", mha_prov_df)

    # MHA Table 6 tag groups
    table_6_1_data = write_excel.prepare_table6_1(main_table6_data, mha_pop_df)
    table_6_2_data = write_excel.prepare_table6_demog(main_table6_data, mha_pop_df, "Gender")
    table_6_3_data = write_excel.prepare_table6_demog(main_table6_data, mha_pop_df, "Age")
    table_6_4_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", white_eth_short)
    table_6_5_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", mixed_eth)
    table_6_6_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", asian_eth)
    table_6_7_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", black_eth)
    table_6_8_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", other_eth_desc)
    table_6_9_data = write_excel.prepare_table6_eth_lower(main_table6_data, mha_pop_df, "Ethnicity", unknown_eth)

    # MHA Main Table 7a tag data groups
    table_7a_1_data = write_excel.prepare_table_age_gender_1(main_table7a_data)
    table_7a_2_data = write_excel.prepare_table_age_gender_2(main_table7a_data)
    table_7a_3_data = write_excel.prepare_table_age_all(main_table7a_data, lower_age)
    table_7a_4_data = write_excel.prepare_table_age_all(main_table7a_data, upper_age)
    table_7a_5_data = write_excel.prepare_table_gender_all(main_table7a_data, gen_desc_all)

    # MHA Main Table 7b tag data groups
    table_7b_1_data = write_excel.prepare_table_eth_1(main_table7b_data)
    table_7b_2_data = write_excel.prepare_table_eth_2(main_table7b_data)
    table_7b_3_data = write_excel.prepare_table_eth_lower(main_table7b_data, white_eth)
    table_7b_4_data = write_excel.prepare_table_eth_lower(main_table7b_data, mixed_eth)
    table_7b_5_data = write_excel.prepare_table_eth_lower(main_table7b_data, asian_eth)
    table_7b_6_data = write_excel.prepare_table_eth_lower(main_table7b_data, black_eth)
    table_7b_7_data = write_excel.prepare_table_eth_lower(main_table7b_data, other_eth)

    # MHA Length of Stay Table 8a tag data groups
    table8a_1_data = write_excel.prepare_lod_all_all(main_table8a_data)
    table8a_2_data = write_excel.prepare_lod_all_part2(main_table8a_data)
    table8a_3_data = write_excel.prepare_lod_all_court(main_table8a_data)
    table8a_4_data = write_excel.prepare_lod_all_pos(main_table8a_data)
    table8a_5_data = write_excel.prepare_lod_demog_all(main_table8a_data, "Gender", gen)
    table8a_6_data = write_excel.prepare_lod_demog_part2(main_table8a_data, "Gender", gen)
    table8a_7_data = write_excel.prepare_lod_demog_court(main_table8a_data, "Gender", gen)
    table8a_8_data = write_excel.prepare_lod_demog_pos(main_table8a_data, "Gender", gen)
    table8a_9_data = write_excel.prepare_lod_demog_all(main_table8a_data, "Age", lod_age)
    table8a_10_data = write_excel.prepare_lod_demog_part2(main_table8a_data, "Age", lod_age)
    table8a_11_data = write_excel.prepare_lod_demog_court(main_table8a_data, "Age", lod_age)
    table8a_12_data = write_excel.prepare_lod_demog_pos(main_table8a_data, "Age", lod_age)
    table8a_13_data = write_excel.prepare_lod_demog_all(main_table8a_data, "IMD Decile", imd)
    table8a_14_data = write_excel.prepare_lod_demog_part2(main_table8a_data, "IMD Decile", imd)
    table8a_15_data = write_excel.prepare_lod_demog_court(main_table8a_data, "IMD Decile", imd)
    table8a_16_data = write_excel.prepare_lod_demog_pos(main_table8a_data, "IMD Decile", imd)
    table8a_17_data = write_excel.prepare_lod_demog_all(main_table8a_data, "Higher Level Ethnicity", lod_upper_eth)
    table8a_18_data = write_excel.prepare_lod_demog_part2(main_table8a_data, "Higher Level Ethnicity", lod_upper_eth)
    table8a_19_data = write_excel.prepare_lod_demog_court(main_table8a_data, "Higher Level Ethnicity", lod_upper_eth)
    table8a_20_data = write_excel.prepare_lod_demog_pos(main_table8a_data, "Higher Level Ethnicity", lod_upper_eth)
    table8a_21_data = write_excel.prepare_lod_demog_all(main_table8a_data, "Lower Level Ethnicity", lod_lower_eth)
    table8a_22_data = write_excel.prepare_lod_demog_part2(main_table8a_data, "Lower Level Ethnicity", lod_lower_eth)
    table8a_23_data = write_excel.prepare_lod_demog_court(main_table8a_data, "Lower Level Ethnicity", lod_lower_eth)
    table8a_24_data = write_excel.prepare_lod_demog_pos(main_table8a_data, "Lower Level Ethnicity", lod_lower_eth)

    # MHA Length of Stay Table 8b tag data groups
    table8b_1_data = write_excel.prepare_lod_all_all(main_table8b_data)
    table8b_2_data = write_excel.prepare_lod_all_part2(main_table8b_data)
    table8b_3_data = write_excel.prepare_lod_all_court(main_table8b_data)
    table8b_4_data = write_excel.prepare_lod_all_pos(main_table8b_data)
    table8b_5_data = write_excel.prepare_lod_all_cto(main_table8b_data)
    table8b_6_data = write_excel.prepare_lod_demog_all(main_table8b_data, "Gender", gen)
    table8b_7_data = write_excel.prepare_lod_demog_part2(main_table8b_data, "Gender", gen)
    table8b_8_data = write_excel.prepare_lod_demog_court(main_table8b_data, "Gender", gen)
    table8b_9_data = write_excel.prepare_lod_demog_pos(main_table8b_data, "Gender", gen)
    table8b_10_data = write_excel.prepare_lod_demog_cto(main_table8b_data, "Gender", gen)
    table8b_11_data = write_excel.prepare_lod_demog_all(main_table8b_data, "Age", lod_age)
    table8b_12_data = write_excel.prepare_lod_demog_part2(main_table8b_data, "Age", lod_age)
    table8b_13_data = write_excel.prepare_lod_demog_court(main_table8b_data, "Age", lod_age)
    table8b_14_data = write_excel.prepare_lod_demog_pos(main_table8b_data, "Age", lod_age)
    table8b_15_data = write_excel.prepare_lod_demog_cto(main_table8b_data, "Age", lod_age)
    table8b_16_data = write_excel.prepare_lod_demog_all(main_table8b_data, "IMD Decile", imd)
    table8b_17_data = write_excel.prepare_lod_demog_part2(main_table8b_data, "IMD Decile", imd)
    table8b_18_data = write_excel.prepare_lod_demog_court(main_table8b_data, "IMD Decile", imd)
    table8b_19_data = write_excel.prepare_lod_demog_pos(main_table8b_data, "IMD Decile", imd)
    table8b_20_data = write_excel.prepare_lod_demog_cto(main_table8b_data, "IMD Decile", imd)
    table8b_21_data = write_excel.prepare_lod_demog_all(main_table8b_data, "Higher Level Ethnicity", lod_upper_eth)
    table8b_22_data = write_excel.prepare_lod_demog_part2(main_table8b_data, "Higher Level Ethnicity", lod_upper_eth)
    table8b_23_data = write_excel.prepare_lod_demog_court(main_table8b_data, "Higher Level Ethnicity", lod_upper_eth)
    table8b_24_data = write_excel.prepare_lod_demog_pos(main_table8b_data, "Higher Level Ethnicity", lod_upper_eth)
    table8b_25_data = write_excel.prepare_lod_demog_cto(main_table8b_data, "Higher Level Ethnicity", lod_upper_eth)
    table8b_26_data = write_excel.prepare_lod_demog_all(main_table8b_data, "Lower Level Ethnicity", lod_lower_eth)
    table8b_27_data = write_excel.prepare_lod_demog_part2(main_table8b_data, "Lower Level Ethnicity", lod_lower_eth)
    table8b_28_data = write_excel.prepare_lod_demog_court(main_table8b_data, "Lower Level Ethnicity", lod_lower_eth)
    table8b_29_data = write_excel.prepare_lod_demog_pos(main_table8b_data, "Lower Level Ethnicity", lod_lower_eth)
    table8b_30_data = write_excel.prepare_lod_demog_cto(main_table8b_data, "Lower Level Ethnicity", lod_lower_eth)

    # Prepare the list of excel tables you want to write
    main_tables = [
        {"sheet_name": "Table 1a", "tag": "tag_table1a_1", "data": table_1a_1_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_2", "data": table_1a_2_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_3", "data": table_1a_3_data}, 
        {"sheet_name": "Table 1a", "tag": "tag_table1a_4", "data": table_1a_4_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_5", "data": table_1a_5_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_6", "data": table_1a_6_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_7", "data": table_1a_7_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_8", "data": table_1a_8_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_9", "data": table_1a_9_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_10", "data": table_1a_10_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_11", "data": table_1a_11_data}, 
        {"sheet_name": "Table 1a", "tag": "tag_table1a_12", "data": table_1a_12_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_13", "data": table_1a_13_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_14", "data": table_1a_14_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_15", "data": table_1a_15_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_16", "data": table_1a_16_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_17", "data": table_1a_17_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_18", "data": table_1a_18_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_19", "data": table_1a_19_data}, 
        {"sheet_name": "Table 1a", "tag": "tag_table1a_20", "data": table_1a_20_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_21", "data": table_1a_21_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_22", "data": table_1a_22_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_23", "data": table_1a_23_data},
        {"sheet_name": "Table 1a", "tag": "tag_table1a_24", "data": table_1a_24_data},
        {"sheet_name": "Table 1b", "tag": "tag_table1b_1", "data": table_1b_1_data},
        {"sheet_name": "Table 1b", "tag": "tag_table1b_2", "data": table_1b_2_data},
        {"sheet_name": "Table 1b", "tag": "tag_table1b_3", "data": table_1b_3_data},
        {"sheet_name": "Table 1b", "tag": "tag_table1b_4", "data": table_1b_4_data},
        {"sheet_name": "Table 1b", "tag": "tag_table1b_5", "data": table_1b_5_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_1", "data": table_1c_1_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_2", "data": table_1c_2_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_3", "data": table_1c_3_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_4", "data": table_1c_4_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_5", "data": table_1c_5_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_6", "data": table_1c_6_data},
        {"sheet_name": "Table 1c", "tag": "tag_table1c_7", "data": table_1c_7_data},
        {"sheet_name": "Table 1d", "tag": "tag_table1d_1", "data": table_1d_1_data},
        {"sheet_name": "Table 1d", "tag": "tag_table1d_2", "data": table_1d_2_data},
        {"sheet_name": "Table 1d", "tag": "tag_table1d_3", "data": table_1d_3_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_1", "data": table_1e_1_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_2", "data": table_1e_2_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_3", "data": table_1e_3_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_4", "data": table_1e_4_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_5", "data": table_1e_5_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_6", "data": table_1e_6_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_7", "data": table_1e_7_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_8", "data": table_1e_8_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_9", "data": table_1e_9_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_10", "data": table_1e_10_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_11", "data": table_1e_11_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_12", "data": table_1e_12_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_13", "data": table_1e_13_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_14", "data": table_1e_14_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_15", "data": table_1e_15_data},
        {"sheet_name": "Table 1e", "tag": "tag_table1e_16", "data": table_1e_16_data},
        {"sheet_name": "Table 1f", "tag": "tag_table1f_1", "data": table_1f_1_data},
        {"sheet_name": "Table 1f", "tag": "tag_table1f_2", "data": table_1f_2_data},
        {"sheet_name": "Table 1f", "tag": "tag_table1f_3", "data": table_1f_3_data},
        {"sheet_name": "Table 1g", "tag": "tag_table1g_1", "data": table_1g_1_data},
        {"sheet_name": "Table 1g", "tag": "tag_table1g_2", "data": table_1g_2_data},
        {"sheet_name": "Table 1g", "tag": "tag_table1g_3", "data": table_1g_3_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_1", "data": table_1h_1_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_2", "data": table_1h_2_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_3", "data": table_1h_3_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_4", "data": table_1h_4_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_5", "data": table_1h_5_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_6", "data": table_1h_6_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_7", "data": table_1h_7_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_8", "data": table_1h_8_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_9", "data": table_1h_9_data},
        {"sheet_name": "Table 1h", "tag": "tag_table1h_10", "data": table_1h_10_data},
        {"sheet_name": "Table 1i", "tag": "tag_table1i_1", "data": table_1i_1_data},
        {"sheet_name": "Table 1i", "tag": "tag_table1i_2", "data": table_1i_2_data},
        {"sheet_name": "Table 1i", "tag": "tag_table1i_3", "data": table_1i_3_data},
        {"sheet_name": "Table 1j", "tag": "tag_table1j_1", "data": table_1j_1_data},
        {"sheet_name": "Table 1j", "tag": "tag_table1j_2", "data": table_1j_2_data},
        {"sheet_name": "Table 1j", "tag": "tag_table1j_3", "data": table_1j_3_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_1", "data": table_2a_1_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_2", "data": table_2a_2_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_3", "data": table_2a_3_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_4", "data": table_2a_4_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_5", "data": table_2a_5_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_6", "data": table_2a_6_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_7", "data": table_2a_7_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_8", "data": table_2a_8_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_9", "data": table_2a_9_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_10", "data": table_2a_10_data},
        {"sheet_name": "Table 2a", "tag": "tag_table2a_11", "data": table_2a_11_data}, 
        {"sheet_name": "Table 2a", "tag": "tag_table2a_12", "data": table_2a_12_data},
        {"sheet_name": "Table 2b", "tag": "tag_table2b_1", "data": table_2b_1_data},
        {"sheet_name": "Table 2b", "tag": "tag_table2b_2", "data": table_2b_2_data},
        {"sheet_name": "Table 2b", "tag": "tag_table2b_3", "data": table_2b_3_data},
        {"sheet_name": "Table 2b", "tag": "tag_table2b_4", "data": table_2b_4_data},
        {"sheet_name": "Table 2b", "tag": "tag_table2b_5", "data": table_2b_5_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_1", "data": table_2c_1_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_2", "data": table_2c_2_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_3", "data": table_2c_3_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_4", "data": table_2c_4_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_5", "data": table_2c_5_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_6", "data": table_2c_6_data},
        {"sheet_name": "Table 2c", "tag": "tag_table2c_7", "data": table_2c_7_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_1", "data": table_3a_1_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_2", "data": table_3a_2_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_3", "data": table_3a_3_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_4", "data": table_3a_4_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_5", "data": table_3a_5_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_6", "data": table_3a_6_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_7", "data": table_3a_7_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_8", "data": table_3a_8_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_9", "data": table_3a_9_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_10", "data": table_3a_10_data},
        {"sheet_name": "Table 3a", "tag": "tag_table3a_11", "data": table_3a_11_data}, 
        {"sheet_name": "Table 3a", "tag": "tag_table3a_12", "data": table_3a_12_data},
        {"sheet_name": "Table 3b", "tag": "tag_table3b_1", "data": table_3b_1_data},
        {"sheet_name": "Table 3b", "tag": "tag_table3b_2", "data": table_3b_2_data},
        {"sheet_name": "Table 3b", "tag": "tag_table3b_3", "data": table_3b_3_data},
        {"sheet_name": "Table 3b", "tag": "tag_table3b_4", "data": table_3b_4_data},
        {"sheet_name": "Table 3b", "tag": "tag_table3b_5", "data": table_3b_5_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_1", "data": table_3c_1_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_2", "data": table_3c_2_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_3", "data": table_3c_3_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_4", "data": table_3c_4_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_5", "data": table_3c_5_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_6", "data": table_3c_6_data},
        {"sheet_name": "Table 3c", "tag": "tag_table3c_7", "data": table_3c_7_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_1", "data": table_4_1_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_2", "data": table_4_2_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_3", "data": table_4_3_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_4", "data": table_4_4_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_5", "data": table_4_5_data},
        {"sheet_name": "Table 4", "tag": "tag_table4_6", "data": table_4_6_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_1", "data": table_5_1_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_2", "data": table_5_2_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_3", "data": table_5_3_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_4", "data": table_5_4_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_5", "data": table_5_5_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_6", "data": table_5_6_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_7", "data": table_5_7_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_8", "data": table_5_8_data},
        {"sheet_name": "Table 5", "tag": "tag_table5_9", "data": table_5_9_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_1", "data": table_6_1_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_2", "data": table_6_2_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_3", "data": table_6_3_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_4", "data": table_6_4_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_5", "data": table_6_5_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_6", "data": table_6_6_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_7", "data": table_6_7_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_8", "data": table_6_8_data},
        {"sheet_name": "Table 6", "tag": "tag_table6_9", "data": table_6_9_data},
        {"sheet_name": "Table 7a", "tag": "tag_table7a_1", "data": table_7a_1_data},
        {"sheet_name": "Table 7a", "tag": "tag_table7a_2", "data": table_7a_2_data},
        {"sheet_name": "Table 7a", "tag": "tag_table7a_3", "data": table_7a_3_data},
        {"sheet_name": "Table 7a", "tag": "tag_table7a_4", "data": table_7a_4_data},
        {"sheet_name": "Table 7a", "tag": "tag_table7a_5", "data": table_7a_5_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_1", "data": table_7b_1_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_2", "data": table_7b_2_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_3", "data": table_7b_3_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_4", "data": table_7b_4_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_5", "data": table_7b_5_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_6", "data": table_7b_6_data},
        {"sheet_name": "Table 7b", "tag": "tag_table7b_7", "data": table_7b_7_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_1", "data": table8a_1_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_2", "data": table8a_2_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_3", "data": table8a_3_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_4", "data": table8a_4_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_5", "data": table8a_5_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_6", "data": table8a_6_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_7", "data": table8a_7_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_8", "data": table8a_8_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_9", "data": table8a_9_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_10", "data": table8a_10_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_11", "data": table8a_11_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_12", "data": table8a_12_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_13", "data": table8a_13_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_14", "data": table8a_14_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_15", "data": table8a_15_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_16", "data": table8a_16_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_17", "data": table8a_17_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_18", "data": table8a_18_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_19", "data": table8a_19_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_20", "data": table8a_20_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_21", "data": table8a_21_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_22", "data": table8a_22_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_23", "data": table8a_23_data},
        {"sheet_name": "Table 8a", "tag": "tag_table8a_24", "data": table8a_24_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_1", "data": table8b_1_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_2", "data": table8b_2_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_3", "data": table8b_3_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_4", "data": table8b_4_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_5", "data": table8b_5_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_6", "data": table8b_6_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_7", "data": table8b_7_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_8", "data": table8b_8_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_9", "data": table8b_9_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_10", "data": table8b_10_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_11", "data": table8b_11_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_12", "data": table8b_12_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_13", "data": table8b_13_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_14", "data": table8b_14_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_15", "data": table8b_15_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_16", "data": table8b_16_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_17", "data": table8b_17_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_18", "data": table8b_18_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_19", "data": table8b_19_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_20", "data": table8b_20_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_21", "data": table8b_21_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_22", "data": table8b_22_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_23", "data": table8b_23_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_24", "data": table8b_24_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_25", "data": table8b_25_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_26", "data": table8b_26_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_27", "data": table8b_27_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_28", "data": table8b_28_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_29", "data": table8b_29_data},
        {"sheet_name": "Table 8b", "tag": "tag_table8b_30", "data": table8b_30_data},
        {"sheet_name": "Table 8c", "tag": "tag_table8c_1", "data": main_table8c_data},     
    ]
    write_excel.write_tables_to_excel(main_tables, main_excel_template, main_excel_output)

    #To produce time series providers submitting chart in CMS (excel file needed to produce chart)
    dq_prov_subm_template = template_dir / 'ment-heal-act-prov-subm-time-series-template.xlsx'
    dq_prov_subm_excel_output = output_dir / f"ment_heal_act_prov_subm_time_series_chart_{year}.xlsx"
    tag_cms_1_data = write_excel.prepare_dq_prov_subm_det_data(csv_dq_prov_subm_det_path, "providers")
    
    # Prepare the list of excel tables you want to write
    dq_prov_subm_table = [
        {"sheet_name": "Sheet1", "tag": "tag_cms_1", "data": tag_cms_1_data},
    ]
    write_excel.write_tables_to_excel(dq_prov_subm_table, dq_prov_subm_template, dq_prov_subm_excel_output)

    #To produce time series detentions chart in CMS (excel file needed to produce chart)
    dq_prov_det_template = template_dir / 'ment-heal-act-det-time-series-template.xlsx'
    dq_prov_det_excel_output = output_dir / f"ment_heal_act_det_time_series_chart_{year}.xlsx"
    tag_cms_1_data = write_excel.prepare_dq_prov_subm_det_data(csv_dq_prov_subm_det_path, "detentions")
    
    # Prepare the list of excel tables you want to write
    dq_prov_det_table = [
        {"sheet_name": "Sheet1", "tag": "tag_cms_1", "data": tag_cms_1_data},
    ]
    write_excel.write_tables_to_excel(dq_prov_det_table, dq_prov_det_template, dq_prov_det_excel_output)

    #To produce time series provider completeness over 12 months chart in CMS (excel file needed to produce chart)
    dq_prov_comp_template = template_dir / 'ment-heal-act-prov-compl-time-series-template.xlsx'
    dq_prov_comp_excel_output = output_dir / f"mha_dq_prov_comp_12m_time_series_chart_{year}.xlsx"
    tag_cms_1_data = write_excel.prepare_dq_prov_comp_data(csv_dq_prov_comp_path, "NHS TRUST")
    tag_cms_2_data = write_excel.prepare_dq_prov_comp_data(csv_dq_prov_comp_path, "INDEPENDENT HEALTH PROVIDER")
    
    # Prepare the list of excel tables you want to write
    dq_prov_comp_table = [
        {"sheet_name": "Sheet1", "tag": "tag_cms_1", "data": tag_cms_1_data},
        {"sheet_name": "Sheet1", "tag": "tag_cms_2", "data": tag_cms_2_data},
    ]
    write_excel.write_tables_to_excel(dq_prov_comp_table, dq_prov_comp_template, dq_prov_comp_excel_output)

    #To produce mhs08 time series chart in CMS (excel file needed to produce chart)
    mhs08_template = template_dir / 'ment-heal-act-mhs08-time-series-template.xlsx'
    mhs08_excel_output = output_dir / f"ment_heal_act_mhs08_time_series_chart_{year}.xlsx"
    tag_cms_1_data = write_excel.prepare_mhs08_data(csv_mhs08_time_series_path)
    
    # Prepare the list of excel tables you want to write
    mhs08_table = [
        {"sheet_name": "Sheet1", "tag": "tag_cms_1", "data": tag_cms_1_data},
    ]
    write_excel.write_tables_to_excel(mhs08_table, mhs08_template, mhs08_excel_output)

    #To produce xlsx files required for CMS charts
    #Detention Type CMS Chart
    cms_det_type_path = output_dir / f"det_type_cms_chart_{year}.xlsx"
    write_excel.prepare_det_type_cms_chart(csv_main_path, cms_det_type_path)
    #Detention Type Percentage CMS Chart
    cms_det_type_perc_path = output_dir / f"det_type_cms_perc_chart_{year}.xlsx"
    write_excel.prepare_det_type_perc_cms_chart(csv_main_path, cms_det_type_perc_path)
    #Detention Count by Ethnicity CMS Chart
    cms_det_eth_count_path = output_dir / f"det_eth_count_chart_{year}.xlsx"
    write_excel.prepare_det_eth_count_cms_chart(csv_main_path, cms_det_eth_count_path)
    #Detention Rate by Ethnicity Excel Chart
    cms_det_eth_rate_path = output_dir / f"det_eth_rate_chart_{year}.xlsx"
    write_excel.prepare_det_eth_rate_cms_chart(csv_main_path, cms_det_eth_rate_path)
    #Detention Rate by Gender CMS Chart
    cms_det_gender_rate_path = output_dir / f"det_gender_rate_chart_{year}.xlsx"
    write_excel.prepare_det_gender_rate_cms_chart(csv_main_path, cms_det_gender_rate_path)
    #Detention Rate by Age group CMS Chart
    cms_det_age_rate_path = output_dir / f"det_age_rate_chart_{year}.xlsx"
    write_excel.prepare_det_age_rate_cms_chart(csv_main_path, cms_det_age_rate_path)
    #Detention Rate by Lower Ethnicity Excel Chart
    cms_det_leth_rate_path = output_dir / f"det_leth_rate_chart_{year}.xlsx"
    write_excel.prepare_det_leth_rate_cms_chart(csv_main_path, cms_det_leth_rate_path)
    #Detention Rate by IMD CMS Chart
    cms_det_imd_rate_path = output_dir / f"det_imd_rate_chart_{year}.xlsx"
    write_excel.prepare_det_imd_rate_cms_chart(csv_main_path, cms_det_imd_rate_path)
    #Detention Rate by IMD CMS Chart
    cms_det_imd_rate_path = output_dir / f"det_imd_rate_chart_{year}.xlsx"
    write_excel.prepare_det_imd_rate_cms_chart(csv_main_path, cms_det_imd_rate_path)
    #Repeat Detentions Count and Percentage CMS Charts
    cms_repeat_det_count_path = output_dir / f"repeat_det_count_chart_{year}.xlsx"
    cms_repeat_det_perc_path = output_dir / f"repeat_det_perc_chart_{year}.xlsx"
    write_excel.prepare_repeat_det_cms_charts(csv_main_path, cms_repeat_det_count_path, cms_repeat_det_perc_path)
    #Repeat Detentions by demographic Excel Charts
    cms_repeat_det_demog_path = output_dir / f"repeat_det_demog_chart_{year}.xlsx"
    write_excel.prepare_repeat_det_demog_cms_charts(csv_main_path, cms_repeat_det_demog_path)
    #Median length of detentions by gender CMS Charts
    cms_median_lod_gender_path = output_dir / f"median_lod_gender_chart_{year}.xlsx"
    write_excel.prepare_median_lod_gender_cms_charts(csv_lod_path, cms_median_lod_gender_path)
    #Median length of detentions by age CMS Charts
    cms_median_lod_age_path = output_dir / f"median_lod_age_chart_{year}.xlsx"
    write_excel.prepare_median_lod_age_cms_charts(csv_lod_path, cms_median_lod_age_path)
    #Median length of detentions by ethnicity CMS Charts
    cms_median_lod_eth_path = output_dir / f"median_lod_eth_chart_{year}.xlsx"
    write_excel.prepare_median_lod_eth_cms_charts(csv_lod_path, cms_median_lod_eth_path)
    #Section 136 Detention Rate by Age group CMS Chart
    cms_s136_det_age_rate_path = output_dir / f"s136_det_age_rate_chart_{year}.xlsx"
    write_excel.prepare_s136_det_age_rate_cms_chart(csv_main_path, cms_s136_det_age_rate_path)
    #Section 136 Detention Rate by Ethnicity Excel Chart
    cms_s136_det_eth_rate_path = output_dir / f"s136_det_eth_rate_chart_{year}.xlsx"
    write_excel.prepare_s136_det_eth_rate_cms_chart(csv_main_path, cms_s136_det_eth_rate_path)
    #Community Treatment Order Rate by Age group CMS Chart
    cms_cto_age_rate_path = output_dir / f"cto_age_rate_chart_{year}.xlsx"
    write_excel.prepare_cto_age_rate_cms_chart(csv_main_path, cms_cto_age_rate_path)
    #Community Treatment Order Rate by Ethnicity Excel Chart
    cms_cto_eth_rate_path = output_dir / f"cto_eth_rate_chart_{year}.xlsx"
    write_excel.prepare_cto_eth_rate_cms_chart(csv_main_path, cms_cto_eth_rate_path)
    #Percentage of people subject to the Act 31st March  by provider type CMS Chart
    cms_mhs08_perc_path = output_dir / f"mhs08_perc_chart_{year}.xlsx"
    write_excel.prepare_mhs08_perc_cms_chart(csv_main_path, cms_mhs08_perc_path)
    #Percentage of people subject to the Act 31st March under different Parts by provider type CMS Chart
    cms_mhs08_prov_type_path = output_dir / f"mhs08_prov_type_chart_{year}.xlsx"
    write_excel.prepare_mhs08_prov_type_cms_chart(csv_mhs08_prov_type_path, cms_mhs08_prov_type_path)

if __name__ == '__main__':
    print(f"Running publication")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    print(f"Running time of create_publication: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.")

# %%
