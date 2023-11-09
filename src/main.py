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

# Window function of kwh / total_kwh_in_bill * service_charge
# Confirm that total_kwh_in_bill is close to kwh_delivered