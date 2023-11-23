from curation import *
from modeling import *

# RAW
scrape_cmp_bills()
scrape_ampion_bills()

# CURATED
### This needs to only try to retrieve the number of columns specified in `cols`
write_results(
    load_data_files(path = "./data/cmp/raw/meter_usage",
                    cols = ["account_number", "service_point_id", "meter_id", "interval_end_datetime", "meter_channel", "kwh"]),
                    dest = "./data/cmp/curated/meter_usage")

write_results(
    load_data_files(path = "./data/cmp/raw/locations"),
                    dest = "./data/cmp/curated/locations")

write_results(
    load_data_files(path = "./data/cmp/raw/bills/parquet",
                    type = 'parquet'),
                    dest = "./data/cmp/curated/bills")

write_results(
    load_data_files(path = "./data/ampion/raw/parquet", 
                    type = 'parquet'), 
                    dest = "./data/ampion/curated")

# # MODELED
# model_dim_datetimes()
# model_dim_meters()
# model_dim_bills()
# model_fct_electric_brew()

# # DATABASE
# connect_to_db()