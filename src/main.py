from utils import *

df = dim_datetimes

if df.columns[0] == 'id':
    df.set_index('id', inplace = True)

print(df)