## Tasks to Tackle
- QA `cmp/raw/bills/scraped_bills.csv` and fill in missing values (and correct erroneous ones due to multiple lines in specific bills)
- Write a `curate_cmp_bills` function to convert `cmp/raw/bills/scraped_bills.csv` to Parquet
- Write a `scrape_ampion_bills` function for ampion and rename the existing `scrape_bills` to `scrape_cmp_bills`
- Write a `curate_ampion_bills` function and a `curate_ampion_energy_production` function
