from utils.runtime import connect_to_db, read_data

'''
Initiailizes commonly used DataFrames (and their supporting database) for easier access across different scripts.
All objects listed below are curated and optimized for efficient data operations.

Variables:
    - meter_usage       (pd.DataFrame) : Contains kWh readings from CMP in as frequent as 15-minute intervals.
    - locations         (pd.DataFrame) : Adds manual CSV entries describing each of Austin Street's accounts
    - cmp_bills         (pd.DataFrame) : Contains billed delivery and supplier rates for various periods of activity.
    - ampion_bills      (pd.DataFrame) : Contains pricing and kWhs supplied by Austin Street's solar provider.
    - dim_datetimes     (pd.DataFrame) : Breaks timestamps into individual date and time components.
    - dim_meters        (pd.DataFrame) : Abstracts account numbers, service points, and streets into one table.
    - dim_bills         (pd.DataFrame) : Unions common dimensions and numerics from `cmp_bills` and `ampion_bills`.
    - fct_electric_brew (pd.DataFrame) : Houses all the model's facts about usage, billing, and the cost of delivery.
    - electric_brew     (dd.DuckDBPyConnection) : Contains pointer views for all of the DataFrames above.
'''

# Curated DataFrames
meter_usage       = read_data("cmp/curated/meter_usage")
locations         = read_data("cmp/curated/locations")
cmp_bills         = read_data("cmp/curated/bills")
ampion_bills      = read_data("ampion/curated/bills")

# Modeled DataFrames
dim_datetimes     = read_data("modeled/dim_datetimes")
dim_meters        = read_data("modeled/dim_meters")
dim_bills         = read_data("modeled/dim_bills")
fct_electric_brew = read_data("modeled/fct_electric_brew")

# DuckDB Database
electric_brew     = connect_to_db()