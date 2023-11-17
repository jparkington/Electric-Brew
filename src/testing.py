from utils import *

'''
- Waterfall `ampion_kwh` as a continuation of `cmp_kwh`
- Facts: Calculate delivery_cost, service_cost, supply_cost, and total_cost
- Ids from both exploded cmp and ampion will need to be coalesced and then renamed
'''

explode = {s: df for s, df in dim_bills.explode('billing_interval')
                                       .assign(date = lambda x: pd.to_datetime(x['billing_interval']))
                                       .groupby('source')}

bill_fields = ['account_number', 'date']
int_df = meter_usage.assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], format = '%m/%d/%Y %I:%M:%S %p')) \
                    .merge(dim_datetimes,     on = 'timestamp', how = 'left', suffixes = ('', '_dat')) \
                    .merge(dim_meters,        on = 'meter_id',  how = 'left', suffixes = ('', '_met')) \
                    .merge(explode['CMP'],    on = bill_fields, how = 'left', suffixes = ('', '_cmp')) \
                    .merge(explode['Ampion'], on = bill_fields, how = 'left', suffixes = ('', '_amp')) \
                    .apply(lambda col: col.fillna(0) if col.dtype.kind in 'biufc' else col)

print(int_df.columns)


# # Add columns to fct_df using calculations from int_df
# int_df['total_recorded_kwh'] = int_df.groupby(['pdf_file_name', 'kwh_delivered'])['kwh'].transform('sum')

# int_df['allocated_service_charge'] = int_df['service_charge'] * int_df['kwh'] / int_df['total_recorded_kwh']

# int_df['delivered_kwh_left'] = int_df.groupby(['pdf_file_name', 'kwh_delivered'], group_keys=False) \
#                                      .apply(lambda g: (g.sort_values(by=['pdf_file_name', 'timestamp'], ascending=True)['kwh_delivered'].iloc[0] - 
#                                                        g.sort_values(by=['pdf_file_name', 'timestamp'], ascending=True)['kwh'].cumsum())
#                                                        .clip(lower=0))

# int_df['delivered_kwh_used'] = np.minimum(int_df['kwh'], int_df['delivered_kwh_left']).clip(lower=0)


fct_df = pd.DataFrame(index = int_df.index)

# Add other columns from int_df as needed
fct_df['dim_datetimes_id'] = int_df['id']
fct_df['dim_meters_id']    = int_df['id_met']
fct_df['dim_bills_id']     = int_df['id_cmp']
fct_df['account_number']   = int_df['account_number']
fct_df['kwh']              = int_df['kwh']

# int_df['total_cost_of_delivery'] = int_df['delivered_kwh_used'] * (int_df['delivery_rate'] + int_df['supply_rate']) + int_df['allocated_service_charge']

# # Step 5: Create a unique identifier `id` for each row
# fct_df.insert(0, 'id', range(1, len(fct_df) + 1))

# print(exploded_cmp, '\n', exploded_ampion)
