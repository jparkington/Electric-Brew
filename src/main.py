from utils.variables import cmp_bills, ampion_bills

common_dims = ['invoice_number', 'account_number', 'interval_start', 'interval_end', 'supplier']

# Standardize `cmp_bills`
df2 = ampion_bills.groupby(common_dims, observed = True) \
                    .agg(kwh_delivered  = ('kwh',   'sum'), 
                        price          = ('price', 'sum')) \
                    .reset_index()
df2['service_charge'] = 0
df2['taxes']          = 0
df2['delivery_rate']  = 0
df2['supply_rate']    = df2['price'] / df2['kwh_delivered']
df2['source']         = "Ampion"

print(df2.dtypes)