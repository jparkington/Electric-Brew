# Ampion PDF Processing
The Ampion bills come in a single pdf for the 7 accounts associated with Austin Street Brewery. This document will detail how the data is extracted.

___
## Data Extraction

Within the ```ampion_utility_functions.py``` file there are a number of helper functions that facilitate the process of data extraction. Here the process will be outlined.

### Utility Functions & Their Uses
* ```open_pdf``` This function will open a one page pdf and extract the text using ```pdf_plumber```. It returns a string, ```text```, containing all the text in the pdf.
* ```find_matches``` This function takes a ```regex``` expression (the query) and a search space as arguements, and returns a list of all discovered instances. It utilizes the regular expression package, ```re```.
* ```execute_regex_search``` This function takes a list of ```regex``` queries and search space as arguements and returns a ```pandas``` dataframe where the columns are the results of each query. It utilizes ```find_matches``` to execute the search.
* ```clean_regex_df``` This function takes the dataframe object returned from ```execute_regex_search``` as an arguement. It cleans and enginears the features and returns a dataframe ready for use.
* ```process_pdf``` This function takes the variable ```text``` that was returned by ```open_pdf``` as an argument. This is where the ```regex``` searches are defined. ```execute_regex_search``` and ```clean_regex_df``` are invoked. A cleaned and processed dataframe is returned.
* ```get_data``` This function iterates over all the pdf files within ```./ampion/raw``` folder. ```open_pdf``` and ```process_pdf``` are called on each file. The results are stored as a list and appended to form ```pandas``` dataframe that contains all the data from all the Ampion bill pdfs.
* ```export_csv_and_parquet_files``` This function will take the pd generated in ```process_pdf```, filter it by account number and export account specific csv and parquet files. These files will be stored in ```ampion/curated/csv_files``` and ```ampion/curated/parquet_files``` respectively.
___
## Reproducibility
To reproduce the curated csv and parquet files from the raw Ampion bill pdfs, execute the following commands.

### Step 1:
This will create the folders, if they do not already exist:
```
make ampion_directories
```
### Step 2:
This will generate the csv and parquet files:
```
make curate_ampion
```


