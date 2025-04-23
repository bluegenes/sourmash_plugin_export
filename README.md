# sourmash_plugin_export

Experimental plugin for exporting sourmash signatures to other formats for exploration

## About

Commands:

- `toparquet` - export rocksdb to parquet format, optionally with taxonomic information


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
pip install .
```

## Running the command
The `toparquet` command is used to export a sourmash signature database to parquet format. It can also include taxonomic information if available.
The command takes the following arguments:
- `--db` - path to the sourmash database (rocksdb)
- `--out` - path to the output parquet file
- `--taxonomy`/`--lineages` - path to the sourmash taxonomic information file mapping genome identifiers to taxonomic lineages (optional; currently only NCBI and GTDB taxonomies are supported)

Now run the `toparquet` command:
```
sourmash scripts toparquet \
    --db /path/to/rocksdb \
    --out /path/to/output.parquet \
    --taxonomy /path/to/taxonomy.csv
```

The output will be a parquet file containing the mapping of hashes in the sourmash sketches to the list of datasets they are found in. If taxonomy information is provided, we will also print the list of taxonomies the hash is found in, plus an LCA of those taxonomic lineages, if one exists. LCA summary information will be printed to the console. The output parquet file can be opened in any tool that supports parquet format, such as pandas or pyarrow.

Example LCA Summary:
```
--- LCA Summary ---
order: 1 (0.0%)
family: 8 (0.0%)
species: 23901 (100.0%)
Total hashes: 23910
-------------------
```

Example parquet file:
| hash              | dataset_names                     | taxonomy_list                      | lca_lineage                        | lca_rank |
|------------------|------------------------------------|------------------------------------|------------------------------------|----------|
| 15249706293397504 | ["GCF_000021665.1 Shewanella b…"] | ["d__Bacteria;p__Proteobacteri…"] | d__Bacteria;p__Proteobacteria;…   | species  |
| 18361245509159168 | ["GCF_000017325.1 Shewanella b…"] | ["d__Bacteria;p__Proteobacteri…"] | d__Bacteria;p__Proteobacteria;…   | species  |
| 584608245878528   | ["GCF_000021665.1 Shewanella b…"] | ["d__Bacteria;p__Proteobacteri…"] | d__Bacteria;p__Proteobacteria;…   | species  |
| 6979370520679168  | ["GCF_000017325.1 Shewanella b…"] | ["d__Bacteria;p__Proteobacteri…"] | d__Bacteria;p__Proteobacteria;…   | species  |
| 3223165789803264  | ["GCF_000021665.1 Shewanella b…"] | ["d__Bacteria;p__Proteobacteri…"] | d__Bacteria;p__Proteobacteria;…   | species  |

