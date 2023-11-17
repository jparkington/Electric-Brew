from utils import *

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

int_df['total_recorded_kwh'] = int_df.groupby(['invoice_number', 'kwh_delivered'])['kwh'].transform('sum')

cmp_waterfall = int_df.sort_values(by = ['invoice_number', 'timestamp'])
int_df['delivered_kwh_left'] = cmp_waterfall.groupby(['invoice_number', 'kwh_delivered'], group_keys = False) \
                                             .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum()).clip(lower = 0))
int_df['delivered_kwh_used'] = np.minimum(int_df['kwh'], int_df['delivered_kwh_left']).clip(lower = 0)

ampion_waterfall = int_df.sort_values(by = ['invoice_number_amp', 'timestamp'])
int_df['ampion_kwh_left'] = ampion_waterfall.groupby(['invoice_number_amp', 'kwh_delivered_amp'], group_keys = False) \
                                            .apply(lambda g: (g['kwh_delivered_amp'].iloc[0] - g['kwh'].cumsum()).clip(lower = 0))
int_df['ampion_kwh_used'] = np.minimum(int_df['kwh'], int_df['ampion_kwh_left']).clip(lower = 0)

# Add other columns from int_df as needed
fct_df = pd.DataFrame(index = int_df.index)
fct_df['dim_datetimes_id'] = int_df['id']
fct_df['dim_meters_id']    = int_df['id_met']
fct_df['dim_bills_id']     = np.where(int_df['ampion_kwh_used'] > 0, int_df['id_amp'], int_df['id_cmp'])
fct_df['account_number']   = int_df['account_number']
fct_df['kwh']              = int_df['kwh']
fct_df['delivery_cost']    = int_df['delivered_kwh_used'] * int_df['delivery_rate']
fct_df['service_cost']     = int_df['service_charge']     * int_df['kwh'] / int_df['total_recorded_kwh']
fct_df['supply_cost']      = int_df['delivered_kwh_used'] * int_df['supply_rate'] + int_df['ampion_kwh_used'] * int_df['supply_rate_amp']
fct_df['total_cost']       = fct_df['delivery_cost'] + fct_df['service_cost'] + fct_df['supply_cost']

# Create a unique identifier `id` for each row
fct_df.insert(0, 'id', range(1, len(fct_df) + 1))
