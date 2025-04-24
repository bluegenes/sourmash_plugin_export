# sourmash_plugin_export

Experimental plugin for exporting sourmash revindex to parquet for exploration

## About

Commands:

- `revindex_to_parquet` - export rocksdb to parquet format, optionally with taxonomic information


## Quickstart

Current install is directly from code

Clone this repository and cd in:
```
git clone https://github.com/bluegenes/sourmash_plugin_export.git
cd sourmash_plugin_export
```

Install dependencies via conda/mamba:
```
mamba env create -f environment.yml
```

Now activate the environment:
```
conda activate sourmash_plugin_export
```

Install the plugin:
```
make all
make install
```

## Running the command
The `revindex_to_parquet` command is used to export a sourmash signature database to parquet format. It can also include taxonomic information if available.
You must provide:
- one or more RocksDB databases to export
- an output path for the parquet file (`--output`)
- (optional) One or more taxonomy CSV files mapping genome identifiers to taxonomic lineages (currently only NCBI and GTDB taxonomies are supported; `--taxonomy`/`--lineages`)
- (optional) A CSV file to output LCA summary information (`--lca-info`)

Now run the `revindex_to_parquet` command:
```
sourmash scripts revindex_to_parquet \
    /path/to/rocksdb \
    --output /path/to/output.parquet \
    --taxonomy /path/to/taxonomy.csv
```

## Example from the test data
To run on the test data included in this repository, you can use the following command from inside the main `sourmash_plugin_export` directory:
```
sourmash scripts revindex_to_parquet tests/test-data/test6.rocksdb --output test6.parquet --taxonomy tests/test-data/test6.taxonomy.csv
```

The output will be a parquet file containing the mapping of hashes in the sourmash sketches to the list of datasets they are found in. If taxonomy information is provided, we will also print an LCA summary to the console and store the taxonomy list and LCA information for each hash in the parquet file (if an LCA exists). The output parquet file can be opened in any tool that supports parquet format, such as pandas with pyarrow.

Example LCA Summary:
```
LCA summary for test6.rocksdb:
--- LCA Summary ---
order: 1 (0.0%)
family: 8 (0.0%)
species: 23901 (100.0%)
Total hashes: 23910
-------------------
```

Example parquet file:
| hash           | dataset_names                                            | taxonomy_list                                           | lca_lineage                                             | lca_rank | ksize | scaled | source_file    |
|----------------|----------------------------------------------------------|----------------------------------------------------------|----------------------------------------------------------|----------|--------|--------|----------------|
| 15249706293397504 | ["GCF_000021665.1 Shewanella baltica OS223"]         | ["d__Bacteria;p__Proteobacteria;..."]                   | d__Bacteria;p__Proteobacteria;...                       | species  | 31     | 1000   | test6.rocksdb  |
| 18361245509159168 | ["GCF_000017325.1 Shewanella baltica OS185"]         | ["d__Bacteria;p__Proteobacteria;..."]                   | d__Bacteria;p__Proteobacteria;...                       | species  | 31     | 1000   | test6.rocksdb  |
| 584608245878528   | ["GCF_000021665.1 Shewanella baltica OS223"]         | ["d__Bacteria;p__Proteobacteria;..."]                   | d__Bacteria;p__Proteobacteria;...                       | species  | 31     | 1000   | test6.rocksdb  |
| 6979370520679168  | ["GCF_000017325.1 Shewanella baltica OS185"]         | ["d__Bacteria;p__Proteobacteria;..."]                   | d__Bacteria;p__Proteobacteria;...                       | species  | 31     | 1000   | test6.rocksdb  |
| 3223165789803264  | ["GCF_000021665.1 Shewanella baltica OS223"]         | ["d__Bacteria;p__Proteobacteria;..."]                   | d__Bacteria;p__Proteobacteria;...                       | species  | 31     | 1000   | test6.rocksdb  |


To look at the output in pandas afterwards, make sure you're in the `sourmash_plugin_export` conda environment or have installed pandas and pyarrow in your current environment. Then you can open python or a jupyter notebook and run the following code to read the parquet file:
```
import pandas as pd
df = pd.read_parquet('test6.parquet')
df
```

If you input more than one RocksDB database, any hashes present in multiple databases will be show up more than once in the output. You can use the `source_file` column to identify which database each hash came from.

## Full Usage

```
usage:  revindex_to_parquet [-h] [-q] [-d] [-o OUTPUT] [-t [TAXONOMY ...]] [--lca-info LCA_INFO] [-c CORES] database [database ...]

export sourmash revindex to parquet, optionally summarizing taxonomic information

positional arguments:
  database              One or more sourmash sketch databases (revindex format only).

options:
  -h, --help            show this help message and exit
  -q, --quiet           suppress non-error output
  -d, --debug           provide debugging output
  -o, --output OUTPUT   Output file name (parquet).
  -t, --taxonomy, --lineages [TAXONOMY ...]
                        One or more taxonomy CSV files (optional).
  --lca-info LCA_INFO   Output an LCA summary to this CSV file.
  -c, --cores CORES     Number of cores to use (default is all available).
  ```
