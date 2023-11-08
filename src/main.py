from utils import *

# 500280 rows
# Convert the interval_end_datetime to a timestamp and select relevant columns
df = meter_usage.assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
                                                              format='%m/%d/%Y %I:%M:%S %p')) \
                 .merge(dim_meters,  on = 'meter_id', how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
                 .merge(dim_datetimes, on = 'timestamp',      how = 'left').rename(columns = {'id' : 'dim_datetimes_id'})

print(df)
# # Expand each row in cmp_bills for each day in its interval
# cmp_bills['interval_date'] = cmp_bills.apply(lambda row: pd.date_range(start=row['interval_start'], end=row['interval_end']).to_list(), axis=1)

# # Explode the date_range to get a row for each date
# cmp_bills_expanded = cmp_bills.explode('interval_date')
# cmp_bills_expanded['interval_date'] = cmp_bills_expanded['interval_date'].dt.date

# # Now, merge with meter_usage on 'account_number' and the date
# merged_df = pd.merge(meter_usage, cmp_bills_expanded, left_on=['account_number', 'interval_date'], 
#                      right_on=['account_number', 'interval_date'], how='left')

# Window function of kwh / total_kwh_in_bill * service_charge
# Confirm that total_kwh_in_bill is close to kwh_delivered