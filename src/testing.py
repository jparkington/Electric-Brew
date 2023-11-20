from utils import *

raw = "./data/ampion/raw/bills"

pdf_data = load_data_files(path = raw, 
                            type = 'PDF')

# Step 3: Use regular expressions to find specific data fields in the extracted text
account_map  = {str(acc)[-8:]: acc for acc in locations['account_number']}
r_invoice    = r"Invoice:\s(\d+)"
r_abbr_acc   = r'\*{5}(\d+)'
r_dates      = r'(\d{2}\.\d{2}\.\d{4})\s*–\s*(\d{2}\.\d{2}\.\d{4})'
r_kwh_values = r'(\d{1,4}(?:,\d{3})*?) kWh'
r_prices     = r'allocated\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})'
records      = []

for _, row in pdf_data.iterrows():

    invoice_number = search(r_invoice,     row['content']).group(1)
    abbr_numbers   = findall(r_abbr_acc,   row['content'])
    dates          = findall(r_dates,      row['content'])
    kwh_values     = findall(r_kwh_values, row['content'])
    prices         = findall(r_prices,     row['content'])

    # Step 5: Create a list of dictionaries containing the scraped data to pass to `pandas`
    records.append({'invoice_number' : invoice_number,
                    'account_number' : account_map.get(abbr_number, abbr_number),
                    'supplier'       : "Ampion",
                    'interval_start' : datetime.strptime(dates[i][0], "%m.%d.%Y").strftime("%Y-%m-%d"),
                    'interval_end'   : datetime.strptime(dates[i][1], "%m.%d.%Y").strftime("%Y-%m-%d"),
                    'kwh'            : int(kwh_values[i].replace(',', '')),
                    'bill_credits'   : prices[i][0],
                    'price'          : prices[i][1] if int(invoice_number[0:4]) < 2023 else prices[i][2]} 
                                        
                    for i, abbr_number in enumerate(range(len(abbr_numbers))))
    
data = pd.DataFrame(records)

data.to_csv('data_test.csv')

'''

account_map isn't working, in that it isn't returning the actual value from the map
Still need logic for `miscellaneous_charges`
                                                    0                                                  1                                                  2                                                  3                                                  4                                                  5                                                  6
0   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
1   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
2   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
3   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
4   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
5   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
6   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
7   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
8   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
9   {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...
10  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...  {'invoice_number': '2023090000800785', 'accoun...

'''