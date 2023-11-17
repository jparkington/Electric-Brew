from utils import *

'''
- Waterfall `ampion_kwh` as a continuation of `cmp_kwh`
- Calculate delivery_cost, service_cost, supply_cost, and total_cost
'''

exploded_ampion = ampion_bills.groupby(['invoice_number', 'account_number', 'interval_start', 'interval_end'], observed = True) \
                              .agg(ampion_kwh = ('kwh', 'sum'), ampion_price = ('price', 'sum')) \
                              .reset_index() \
                              .assign(ampion_supply_rate = lambda df: df['ampion_price'] / df['ampion_kwh'],
                                      date = lambda df: df.apply(
                                             lambda row: pd.date_range(start = row['interval_start'], 
                                                                       end   = row['interval_end'])
                                                           .to_list(), axis = 1)) \
                              .explode('date')

exploded_cmp = cmp_bills.assign(date = lambda df: df.apply(
                                       lambda row: pd.date_range(start = row['interval_start'], 
                                                                 end   = row['interval_end'])
                                                     .to_list(), axis = 1)) \
                        .explode('date')

int_df = meter_usage.drop('account_number', axis = 1) \
                    .assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
                                                                  format = '%m/%d/%Y %I:%M:%S %p')) \
                    .merge(dim_meters,      on = 'meter_id',  how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
                    .merge(dim_datetimes,   on = 'timestamp', how = 'left').rename(columns = {'id' : 'dim_datetimes_id'}) \
                    .merge(exploded_cmp,    on = ['account_number', 'date'], how = 'left') \
                    .merge(exploded_ampion, on = ['account_number', 'date'], how = 'left') \
                #     .merge(dim_suppliers,   on = 'supplier',  how = 'left').rename(columns = {'id' : 'dim_suppliers_id'})

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
