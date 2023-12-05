from utils.dataframes import *

df = dim_datetimes[dim_datetimes['date'] >= '2023-07-01']

df.to_csv('dim_datetimes.csv')
dim_bills.to_csv('dim_bills.csv')
dim_meters.to_csv('dim_meters.csv')
fct_electric_brew.merge(df, left_on = 'dim_datetimes_id', right_on = 'id').to_csv('fct_electric_brew.csv')
