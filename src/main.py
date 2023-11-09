from utils import *

# Convert the interval_end_datetime to a timestamp and select relevant columns

exploded_bills = cmp_bills.assign(date = lambda df: 
                                  df.apply(lambda row: pd.date_range(start = row['interval_start'], 
                                                                     end   = row['interval_end']) \
                                                         .to_list(), axis = 1)) \
                                                         .explode('date')

df = meter_usage.drop('account_number', axis = 1) \
                .assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
                                                              format = '%m/%d/%Y %I:%M:%S %p')) \
                .merge(dim_meters,     on = 'meter_id',  how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
                .merge(dim_datetimes,  on = 'timestamp', how = 'left').rename(columns = {'id' : 'dim_datetimes_id'}) \
                .merge(exploded_bills, on = ['account_number', 'date'], how = 'left') \
                .merge(dim_suppliers,  on = 'supplier',  how = 'left').rename(columns = {'id' : 'dim_suppliers_id'}) \
                [['dim_datetimes_id',
                  'dim_meters_id',
                  'dim_suppliers_id',
                  'kwh',
                  'service_charge',
                  'delivery_rate',
                  'supply_rate',
                  'kwh_delivered',
                  'pdf_file_name']]

print(exploded_bills)

# With a window function, start at the end of the `meter_usage` based timestamps according to the range, and programatically sum-up until `running_kwh` reaches `kwh_delivered`. As the sum is going, preserve `kwh` as `kwh_billed`, which will have the rates applied to it.
# `allocated_service_charge` will be service_charge * (`kwh` / `total_kwh`)