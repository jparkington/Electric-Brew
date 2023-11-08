from utils import *

import pandas as pd

meter_usage['interval_date'] = pd.to_datetime(meter_usage['interval_end_datetime'], format = '%m/%d/%Y %I:%M:%S %p').dt.date

# Expand each row in cmp_bills for each day in its interval
cmp_bills['interval_date'] = cmp_bills.apply(lambda row: pd.date_range(start=row['interval_start'], end=row['interval_end']).to_list(), axis=1)

# Explode the date_range to get a row for each date
cmp_bills_expanded = cmp_bills.explode('interval_date')
cmp_bills_expanded['interval_date'] = cmp_bills_expanded['interval_date'].dt.date

# Now, merge with meter_usage on 'account_number' and the date
merged_df = pd.merge(meter_usage, cmp_bills_expanded, left_on=['account_number', 'interval_date'], 
                     right_on=['account_number', 'interval_date'], how='left')

print(merged_df)

# Window function of kwh / total_kwh_in_bill * service_charge
# Confirm that total_kwh_in_bill is close to kwh_delivered