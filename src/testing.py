from utils import *

raw = "./data/cmp/raw/bills"

pdf_data = load_data_files(path = raw, 
                           type = 'PDF')

def extract_field(pattern, replace_dict = None):
    search_result = search(pattern, pdf_text)
    field_value   = search_result.group(1) if search_result else "NULL"
    
    if replace_dict:
        field_value = "".join(replace_dict.get(char, char) for char in field_value)
        
    return field_value

meter_details = search(r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges", 
                        pdf_text, 
                        DOTALL)

records.append({'invoice_number' : os.path.basename(pdf_path).split('_')[0],
                'account_number' : extract_field(r"Account Number\s*([\d-]+)", {"-": ""}),
                'supplier'       : "",
                'amount_due'     : extract_field(r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"),
                'service_charge' : extract_field(r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})", {"$": "", "+": ""}),
                'kwh_delivered'  : meter_details.group(3).replace(",", "") if meter_details else "NULL",
                'delivery_rate'  : extract_field(r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"),
                'supply_rate'    : "",
                'interval_start' : datetime.strptime(meter_details.group(1), "%m.%d.%Y").strftime("%Y-%m-%d") if meter_details else "NULL",
                'interval_end'   : datetime.strptime(meter_details.group(2), "%m.%d.%Y").strftime("%Y-%m-%d") if meter_details else "NULL",
                'total_kwh'      : ""})
