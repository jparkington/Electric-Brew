<!-- omit in toc -->
# Directory Documentation for `/src/`

The `/src/` directory houses various utility scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. Our instance of `/src/` houses scripts responsible for data curation, output formatting, and visualization. This document provides a brief overview of each script and its contained functions.

<!-- omit in toc -->
## Table of Contents

- [`utils.py`](#utilspy)
  - [`set_plot_params`](#set_plot_params)
  - [`curate_meter_usage`](#curate_meter_usage)

## `utils.py`

The `utils.py` script provides a variety of utility functions spanning different aspects of the project, from visual parameter configuration to comprehensive data curation tasks.

### `set_plot_params`

**Purpose** Initializes and returns custom plotting parameters for `matplotlib`, ensuring consistent visual style throughout the project.

**Signature** 
```python
def set_plot_params() -> list:
```

**Returns** A list containing RGBA color tuples that comprise the custom color palette for plots.

### `curate_meter_usage`

**Purpose** Processes all CSV files from the provided directory, integrates the specified schema, and subsequently consolidates the data into a partitioned `.parquet` file in the designated output directory. This conversion and curation process is optimized with `snappy` compression for efficiency.

**Signature** 
```python
def curate_meter_usage(raw           : str, 
                       curated       : str, 
                       partition_col : list, 
                       schema        : list):
```

**Parameters**

- **`raw`**: Path to the directory containing raw `meter-usage` CSV files.

- **`curated`** : Directory where the consolidated `.parquet` files will be saved.

- **`partition_col`**: Columns by which the .parquet files will be partitioned.

- **`schema`**: Columns that will be used as headers in the resulting DataFrame.