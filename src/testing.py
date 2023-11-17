from utils import *

'''
- Waterfall `ampion_kwh` as a continuation of `cmp_kwh`
- Facts: Calculate delivery_cost, service_cost, supply_cost, and total_cost
- Ids from both exploded cmp and ampion will need to be coalesced and then renamed
'''

# exploded = {s: df for s, df in dim_bills.explode('billing_interval').groupby('source')}


# int_df = meter_usage.drop('account_number', axis = 1) \
#                     .assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
#                                                                   format = '%m/%d/%Y %I:%M:%S %p')) \
#                     .merge(dim_meters,      on = 'meter_id',  how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
#                     .merge(dim_datetimes,   on = 'timestamp', how = 'left').rename(columns = {'id' : 'dim_datetimes_id'}) \
#                 #     .merge(exploded_bills,    on = ['account_number', 'date'], how = 'left') \
#                 #     .merge(exploded_ampion, on = ['account_number', 'date'], how = 'left') \

# # print(exploded['CMP'])

# common_dims = ['invoice_number', 'account_number', 'interval_start', 'interval_end', 'supplier']

# # Standardize `cmp_bills`
# df1 = cmp_bills.groupby(common_dims, observed=True) \
#                 .agg(kwh_delivered  = ('kwh_delivered',  'sum'),
#                         service_charge = ('service_charge', 'sum'), 
#                         delivery_rate  = ('delivery_rate',  'mean'),
#                         supply_rate    = ('supply_rate',    'mean')) \
#                 .reset_index()

# print(df1, '\n', cmp_bills)

# Do the .fillna differently
                #     .fillna({'kwh_delivered'      : 0,
                #              'service_charge'     : 0,
                #              'delivery_rate'      : 0, 
                #              'supply_rate'        : 0,
                #              'ampion_kwh'         : 0,
                #              'ampion_supply_rate' : 0})

# Decompose the .assign and use key assignment
# fct_df = (
#     int_df.sort_values(by = ['pdf_file_name', 'timestamp']) \
#           .assign(
#         total_recorded_kwh       = int_df.groupby(['pdf_file_name', 'kwh_delivered'])['kwh'].transform('sum'),
#         allocated_service_charge = lambda x: x['service_charge'] * x['kwh'] / x['total_recorded_kwh'],
#         delivered_kwh_left       = int_df.groupby(['pdf_file_name', 'kwh_delivered'], group_keys = False)
#                                             .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum())
#                                             .clip(lower = 0)),
#         delivered_kwh_used       = lambda x: np.minimum(x['kwh'], x['delivered_kwh_left']).clip(lower = 0),
#         total_cost_of_delivery   = lambda x: x['delivered_kwh_used'] * (x['delivery_rate'] + x['supply_rate']) +
#                                                 x['allocated_service_charge']))
        # .sort_values(by = ['dim_meters_id', 'dim_datetimes_id']) \
        # [[
        #         'dim_datetimes_id',
        #         'dim_meters_id',
        #         'dim_suppliers_id',
        #         'account_number',
        #         'kwh',
        #         'total_cost_of_delivery'
        # ]]


# # Step 5: Create a unique identifier `id` for each row
# fct_df.insert(0, 'id', range(1, len(fct_df) + 1))

# print(exploded_cmp, '\n', exploded_ampion)
