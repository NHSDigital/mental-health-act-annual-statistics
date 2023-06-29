# Here go the backtesting parameters

bt_params = {
    'OUTPUT_DIR': r'\\Ic\IC_DME_DFS\DME_PROD\WESR\WESR_ESR_EXTRACT\ANALYSIS\WKFC\OLD_SASDEVSERVER\ESR Absence\RAP\Tests\Backtesting\Outputs_to_test',
    'GROUND_TRUTH_DIR': r'\\Ic\IC_DME_DFS\DME_PROD\WESR\WESR_ESR_EXTRACT\ANALYSIS\WKFC\OLD_SASDEVSERVER\ESR Absence\RAP\Tests\Backtesting\Ground_truth',
    'files_to_compare': [
                            # Benchmarking
                            ('benchmarking_csv_2021-12-31.csv', 'Benchmarking_from_sql_doctors_replaced.csv'),
                            # Absence
                            ('csv_absence_rates_2021-12-31.csv', 'ESR_ABSENCE_CSV_NHSE_hf_org_replaced.csv'),
                            # Covid
                            ('covid_2021-12-31.csv', 'MDS_ABSENCE_CSV_COVID19_hf.csv'),
                            # Reason by absence
                            # ('reason_absence_2021-12-31.csv', 'MDS_ABSENCE_CSV_REASON_hf.csv'),
                        ]
    }
