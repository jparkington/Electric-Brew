<!-- omit in toc -->
# Directory Documentation for `/cmp/curated`

Within this directory, we have structured and optimized our data collected from CMP using the Parquet format. This README provides a brief rationale for our data storage decisions and offers a few code examples to help you navigate the data efficiently.

## Why Parquet?

- **Compression**: With Parquet's efficient columnar storage design, we not only save on storage space but also achieve faster read times.

- **Logical Partitions**: Our `meter_data` directory is partitioned by account number. This structure enables rapid data retrieval, especially when dealing with large datasets.

- **Integration with Pandas**: As a team that relies heavily on Pandas, the ease with which Parquet interfaces with dataframes is a boon to our analytical workflows.

## Usage Guidelines

Here are a couple of examples to help you get started with the Parquet format, if you haven't used it before.

### Reading the Complete Dataset

If you'd like to load the entire dataset into a dataframe, with all of its partitions, you can use the `pyarrow.parquet` method for `read_table`, which will automatically traverse the directories and grab any Parquet files it finds.

**Note**: This example assumes you're running your script from the root folder.

```python
import pyarrow.parquet as pq

path = 'data/cmp/curated/meter-usage'
complete_dataset_df = pq.read_table(path).to_pandas()
```

### Accessing Specific Partitions

Should you need data from a specific account numnber, you have two options. Let's assume the account number of interest is `30010320353`. When partitioning, Parquet includes the field name in each partition directory, as you'll see in the examples below.

For the more standard way of retrieving partition directory, you can mirror the structure for retrieving the full dataset.

```python
import pyarrow.parquet as pq

path = 'data/cmp/curated/meter-usage/account_number=30010320353'
specific_partition_df = pq.read_table(path).to_pandas()
```

### Reading a Specific Parquet File

Otherwise, in our case, since the dataset volume isn't exceedingly large, each of our partitions are likely to have a single `.parquet` file inside of them. Pandas allows the user to read a specific `.parquet` file with the `read_parquet` method.

Here's what that would look like if the file were named `fe0d47eb5e324a3a827156a7be2ff434-0.parquet`, which is typically what the naming structure looks like (`{hash}-{file_index_for_this_partition}`).

```python
import pandas as pd

path = './curated/meter_data/account_number=12345/fe0d47eb5e324a3a827156a7be2ff434-0.parquet'
specific_file_df = pd.read_parquet(path)
```