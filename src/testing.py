from utils.curation import *
import os
import re
from datetime import datetime

def extract_records_from_cmp_bills(raw: str = "./data/cmp/raw/bills"):
    '''
    This function reads all PDFs in the specified `raw` directory, extracts specific information from CMP bills using 
    regular expressions, and then returns a list of dictionaries containing the scraped data.

    Methodology:
        1. Load data from PDF files in the `raw` directory using `load_data_files`.
        2. Extract relevant fields from each page of the bill using regular expressions.
        3. Create a list of dictionaries, each representing a record from a single delivery group within a bill.
        4. Each record contains fields such as invoice number, account number, amount due, delivery tax, and supplier information.

    Parameters:
        raw (str): Path to the directory containing CMP bill PDF files.
    '''

    try:
        # Step 1: Load data from PDF files
        pdf_data = load_data_files(path = raw, 
                                   type = 'PDF')
        
        def better_search(pattern, text, default = ""):
            '''
            Searches for a pattern in text and returns either the first group if found or the default value.
            '''

            match = re.search(pattern, text, re.DOTALL)
            return match.group(1) if match else default

        # Regular expressions for data fields
        r_amount_due       = r"Amount Due.*?\$\s*(\d+\.\d{2})"
        r_delivery_tax     = r"Maine Sales Tax \+\$(\d+\.\d{2})"
        r_delivery_group   = r"Delivery Charges:.*?\(\s*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\s*\)"
        r_service_charge   = r"Service Charge.*?\+\$(\d+\.\d{2})"
        r_delivery_service = r"Delivery Service: ([\d,]+) KWH (?:@\$\d+\.\d{6} )?\+\$(\d+\.\d{2})"
        r_supplier_info    = r"Prior Balance for ([A-Z\s\w.]+)(?: Supplier)? \$\d+\.\d{2}"
        r_kwh_supplied     = r"Energy Charge ([\d,]+) KWH"
        r_supply_charge    = r"Energy Charge.*?\+\$(\d+\.\d{2})"
        r_supply_tax       = r"Maine Sales Tax \+\$(\d+\.\d{2})"

        # Step 2: Extract data using regular expressions
        records = []
        for file_path, row in pdf_data.groupby("file_path"): # Adding records file-by-file

            # Some string patterns can only exist on certain pages, so this helps narrow the search
            page = {n: s for n, s in zip(row['page_number'], row['content'])}

            invoice_number = os.path.basename(file_path).split('_')[0]
            account_number = os.path.basename(os.path.dirname(file_path))
            amount_due     = better_search(r_amount_due,   page.get(1))
            delivery_tax   = better_search(r_delivery_tax, page.get(2))

            delivery_groups = re.findall(r_delivery_group, page.get(2), re.DOTALL)
            for start, end in delivery_groups:

                # Find content for the current delivery group
                delivery_content = re.search(rf"({start}.*?)({end}).*?(?=\s*Delivery Charges:|\Z)", 
                                             page.get(2), 
                                             re.DOTALL).group()

                service_charge  = better_search(r_service_charge, delivery_content)
                delivery_search = re.search(r_delivery_service, delivery_content, re.DOTALL)
                kwh_delivered   = delivery_search.group(1).replace(",", "") if delivery_search else ""
                delivery_charge = delivery_search.group(2) if delivery_search else ""

                # Step 3: Extract supplier information
                supplier = kwh_supplied = supply_charge = supply_tax = ""
                supplier_page = next((n for n, s in page.items() 
                                    if n >= 3 and re.search(r_supplier_info, s, re.DOTALL)), None)

                if supplier_page:
                    supplier_content = page[supplier_page]
                    supplier = better_search(r_supplier_info, supplier_content).strip().replace(" Supplier", "")
                    kwh_supplied = better_search(r_kwh_supplied,  supplier_content).replace(",", "")
                    supply_charge = better_search(r_supply_charge, supplier_content)
                    supply_tax = better_search(r_supply_tax,    supplier_content)

                # Step 4: Create records
                records.append({'invoice_number'  : invoice_number,
                                'account_number'  : account_number,
                                'amount_due'      : amount_due,
                                'delivery_tax'    : delivery_tax,
                                'interval_start'  : datetime.strptime(start.strip(), "%m/%d/%Y").strftime("%Y-%m-%d"),
                                'interval_end'    : datetime.strptime(end.strip(),   "%m/%d/%Y").strftime("%Y-%m-%d"),
                                'service_charge'  : service_charge,
                                'kwh_delivered'   : kwh_delivered,
                                'delivery_charge' : delivery_charge,
                                'supplier'        : supplier,
                                'kwh_supplied'    : kwh_supplied,
                                'supply_charge'   : supply_charge,
                                'supply_tax'      : supply_tax})

        return records

    except Exception as e:
        print(f"Error while processing CMP bills: {e}")

# Example usage of the function
sample_records = extract_records_from_cmp_bills()
pd.DataFrame(sample_records).to_csv('test_output.csv')
