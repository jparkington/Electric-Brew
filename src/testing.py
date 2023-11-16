from utils import *

# exploded_ampion = ampion_bills.assign(date = lambda df: 
#                         df.apply(lambda row: pd.date_range(start = row['interval_start'], 
#                                                            end   = row['interval_end'])
#                                                 .to_list(), axis = 1)) \
#                                                 .explode('date')

# exploded_cmp = cmp_bills.assign(date = lambda df: 
#                         df.apply(lambda row: pd.date_range(start = row['interval_start'], 
#                                                            end   = row['interval_end'])
#                                                 .to_list(), axis = 1)) \
#                                                 .explode('date')

# print(ampion_bills)


'''
- Employ a groupby strategy for both resulting in the same dimension column names and with facts filled with 0s in place of nulls
- Calculate `supply_rate` in `ampion_bills` while doing so
- Join first? Then explode once?
- Might need to confirm that interval_start and interval_end lines up with existing values from `cmp_bills` and then use those `total_kwh` values'''

# Step 2: Merge and curate intermediary DataFrame
# int_df = meter_usage.drop('account_number', axis = 1) \
#                     .assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
#                                                                   format = '%m/%d/%Y %I:%M:%S %p')) \
#                     .merge(dim_meters,      on = 'meter_id',  how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
#                     .merge(dim_datetimes,   on = 'timestamp', how = 'left').rename(columns = {'id' : 'dim_datetimes_id'}) \
#                     .merge(exploded_cmp,    on = ['account_number', 'date'], how = 'left') \
#                     .merge(exploded_ampion, on = ['account_number', 'date'], how = 'left') \
#                     .merge(dim_suppliers,   on = 'supplier',  how = 'left').rename(columns = {'id' : 'dim_suppliers_id'}) \
#                     .sort_values(by = ['pdf_file_name', 'timestamp']) \
#                     .fillna({'kwh_delivered'  : 0,
#                              'service_charge' : 0,
#                              'delivery_rate'  : 0, 
#                              'supply_rate'    : 0})

# # Steps 3 & 4: Calculate metrics and total cost of delivery
# fct_df = (
#     int_df.assign(
#         total_recorded_kwh       = int_df.groupby(['pdf_file_name', 'kwh_delivered'])['kwh'].transform('sum'),
#         allocated_service_charge = lambda x: x['service_charge'] * x['kwh'] / x['total_recorded_kwh'],
#         delivered_kwh_left       = int_df.groupby(['pdf_file_name', 'kwh_delivered'], group_keys = False)
#                                             .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum())
#                                             .clip(lower = 0)),
#         delivered_kwh_used       = lambda x: np.minimum(x['kwh'], x['delivered_kwh_left']).clip(lower = 0),
#         total_cost_of_delivery   = lambda x: x['delivered_kwh_used'] * (x['delivery_rate'] + x['supply_rate']) +
#                                                 x['allocated_service_charge'])) \
#         .sort_values(by = ['dim_meters_id', 'dim_datetimes_id']) \
#         [[
#             # Relational and partition keys
#             'dim_datetimes_id',
#             'dim_meters_id',
#             'dim_suppliers_id',
#             'account_number',

#             # Established facts from `meter_usage` and `cmp_bills`
#             'kwh',
#             'service_charge',
#             'delivery_rate',
#             'supply_rate',

#             # Newly curated facts
#             'allocated_service_charge',
#             'delivered_kwh_left',
#             'delivered_kwh_used',
#             'total_cost_of_delivery'
#         ]]

# # Step 5: Create a unique identifier `id` for each row
# fct_df.insert(0, 'id', range(1, len(fct_df) + 1))

# print(exploded_cmp, '\n', exploded_ampion)
