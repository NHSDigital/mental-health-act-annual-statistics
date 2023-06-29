# Mental Health Act Annual Statistics

**_Warning - this repository is a snapshot of a repository internal to NHS England. This means that links to videos and some URLs may not work._**

**Repository owner:** Analytical Services: Community and Mental Health

**Email:** mh.analysis@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

## Introduction

This codebase is used in the creation of the Mental Health Act Annual Statistics publication. This repository includes all of the code used to create the CSV and the associated outputs. The publication uses the Mental Health Services Dataset (MHSDS), further information about the dataset can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set.

The full publication series can be found at https://digital.nhs.uk/data-and-information/publications/statistical/mental-health-act-statistics-annual-figures. 

Other Mental Health related publications and dashboards can be found at the Mental Health Data Hub: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub 

## Folder structure

The repository is structured as follows:
```bash
│   .gitignore
│   config.toml
│   environment.yml
│   README.md
│
├───.vscode
│       launch.json
│
├───data
│       .gitignore
│
├───excel_templates
│       ment-heal-act-det-time-series-template.xlsx
│       ment-heal-act-mhs08-time-series-template.xlsx
│       ment-heal-act-prov-compl-time-series-template.xlsx
│       ment-heal-act-prov-subm-time-series-template.xlsx
│       ment-heal-act-stat-eng-main-template.xlsx
│
├───log
│       .gitignore
│
├───mha_code
│   │   demog_params.py
│   │   helpers.py
│   │   make_publication.py
│   │   write_excel.py
│   │   __init__.py
│   │
│   ├───.vscode
│   │       launch.json
│   │
│   ├───ECDS
│   │       ECDS_Mental_Health_Classification_Code_v1.sql
│   │       ECDS_Mental_Health_Classification_Code_v2.sql
│   │
│   └───MHSDS Code
│       │   all_tables.py
│       │   dq.py
│       │   main.py
│       │   mhsds_functions.py
│       │   output.py
│       │   population.py
│       │
│       ├───agg
│       │       discharges_following_detention_agg.py
│       │       ecds_agg.py
│       │       end_of_fy_year_agg.py
│       │       mha_cto_los_agg.py
│       │       mha_los_only_agg.py
│       │       repeat_detentions_agg.py
│       │       table1_agg.py
│       │       table2_agg.py
│       │       table3_agg.py
│       │       table4_agg.py
│       │       transfers_agg.py
│       │
│       ├───prep
│       │       annual_measures_prep.py
│       │       ecds_prep.py
│       │       monthly_measures_prep.py
│       │
│       └───rates
│               age_ctos_cr.py
│               age_detentions_cr.py
│               age_discharges_cr.py
│               age_stos_cr.py
│               all_cr.py
│               ccg_detentions_cr.py
│               eth_ctos_sr.py
│               eth_detentions_sr.py
│               eth_discharges_sr.py
│               eth_stos_sr.py
│               gender_ctos_cr.py
│               gender_detentions_cr.py
│               gender_discharges_cr.py
│               gender_stos_cr.py
│               imd_detentions_cr.py
│               rates_combine.py
│               stp_detentions_cr.py
│
├───output
│       .gitignore
│
└───tests
    │   __init__.py
    │
    └───backtesting
            backtesting_params.py
            test_compare_outputs.py
            __init__.py
```

## Installation and running
Please note that the code included in this project is designed to be run on Databricks within the NHS England systems. As such some of the code included here may not run on other MHSDS assets. The logic and methods used to produce the metrics included in this codebase remain the same though. 

To set up the Absence Rates package enter the commands below in Anaconda Prompt (terminal on Mac/Linux):
```
conda env create -f environment.yml

conda activate mha-env
```
- _More on virtual environments: [Guide](https://github.com/NHSDigital/rap-community-of-practice/blob/main/python/virtual-environments.md)_

## Understanding the Mental Health Services Dataset

MHSDS is collected on a monthly basis from providers of secondary mental health services. On average around 210 million rows of data flow into the dataset on a monthly basis. More information on the data quality of the dataset, including the numbers of providers submitting data and the volumes of data flowing to each table can be found in the Data Quality Dashboard: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub/data-quality/mental-health-services-dataset---data-quality-dashboard 

The MHSDS tables and fields used within the code are all documented within the MHSDS tools and guidance. This guidance can be found here: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance

Within the guidance are three key documents:

1) MHSDS Technical Output Specification - This provides technical details of all of the fields and tables contained within the dataset. It also contains details of the validations applied to specific tables and fields. The specification also includes details of derivations and how they are constructed.
2) MHSDS Data Model - This details all of the tables and fields within the dataset and how they relate to each other.
3) MHSDS User Guidance - This document provides details of all of the tables and fields within the dataset and gives examples of how a user might populate the data in certain scenarios.

Additionally, users might want to consult the Data Dictionary for specific fields within the dataset: https://www.datadictionary.nhs.uk/ 

## Appendix and Notes

In places the notebooks above use some acronyms. The main ones used are as follows:

- MHSDS: Mental Health Services Dataset
- CCG: Clinical Commissioning Group. These were replaced by Sub Integrated Care Boards (ICBs) in July 2022.
- ICB: Integrated Care Board. These came into effect on July 1st 2022. Further information can be found at https://www.kingsfund.org.uk/publications/integrated-care-systems-explained#development.
- Provider: The organisation is providing care. This is also the submitter of MHSDS data
-LA: Local Authority

## Support
If you have any questions or issues regarding the constructions or code within this repository please contact mh.analysis@nhs.net

## Authors and acknowledgment
Community and Mental Health Team, NHS England
mh.analysis@nhs.net

## License
The menh_bbrb codebase is released under the MIT License.
The documentation is © Crown copyright and available under the terms of the [Open Government 3.0] (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
