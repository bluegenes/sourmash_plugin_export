import argparse
import polars as pl
import pandas as pd
import numpy as np
import os
from collections import defaultdict
from typing import List, Tuple, Optional

def compute_lca_strs(taxonomy_list: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not taxonomy_list:
        return None, None

    split_lineages = [lineage.split(";") for lineage in taxonomy_list if lineage]
    
    if not split_lineages:
        return None, None

    # Find the shortest lineage to avoid index errors
    min_len = min(len(lineage) for lineage in split_lineages)

    lca = []
    for i in range(min_len):
        ith_values = [lineage[i] for lineage in split_lineages]
        if all(val == ith_values[0] for val in ith_values):
            lca.append(ith_values[0])
        else:
            break

    if not lca:
        return None, None

    lca_lineage = ";".join(lca)
    lca_rank = lca[-1].split("__")[0] if "__" in lca[-1] else None

    return lca_lineage, lca_rank


def merge_taxonomic_info_pandas(df: pl.DataFrame) -> pl.DataFrame:
    duplicate_hashes = (
        df.group_by(["hash", "ksize"])
        .agg(pl.count("hash").alias("count"))
        .filter(pl.col("count") > 1)
        .select(["hash", "ksize"])
    )

    df_duplicates = df.join(duplicate_hashes, on=["hash", "ksize"], how="inner")
    df_unique = df.join(duplicate_hashes, on=["hash", "ksize"], how="anti")
    print(f"Found {len(df_duplicates)} row(s) with duplicated hashes and {len(df_unique)} unique row(s).")

    pdf = df_duplicates.to_pandas()
    grouped = pdf.groupby(["hash", "ksize"], sort=False)
    merged_rows = []

    for (hash_val, ksize_val), group in grouped:
        all_names = sorted(set(n for lst in group["dataset_names"] for n in lst))
        all_taxonomies = [
            t
            for entry in group["taxonomy_list"]
            if entry is not None
            for t in (
                entry.tolist() if isinstance(entry, np.ndarray)
                else entry if isinstance(entry, list)
                else [entry]
            )
        ]
        scaled_vals = group["scaled"].unique()
        scaled = scaled_vals[0] if len(scaled_vals) == 1 else None
        lca_lineage, lca_rank = compute_lca_strs(all_taxonomies) if all_taxonomies else (None, None)
        merged_rows.append({
            "hash": hash_val,
            "ksize": ksize_val,
            "scaled": scaled,
            "dataset_names": all_names,
            "taxonomy_list": all_taxonomies or None,
            "lca_lineage": lca_lineage,
            "lca_rank": lca_rank,
            "source": ";".join(sorted(set(group["source"].tolist())))
        })
    # print summary of merged rows
    print(f"Merged {len(df_duplicates)} rows with duplicated hashes into {len(grouped)} unique rows.")

    merged_dups = pl.from_dicts(merged_rows)
    final_df = pl.concat([df_unique, merged_dups], how="vertical")
    return final_df

def main(args):
    df = pl.read_parquet(args.parquet_file)

    merged_df = merge_taxonomic_info_pandas(df)

    if args.output:
        merged_df.write_parquet(args.output)
        print(f"Wrote merged results to: {args.output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge duplicate hash entries with different sources in a Parquet file.")
    parser.add_argument("parquet_file", help="Input Parquet file")
    parser.add_argument("-o", "--output", help="Optional output Parquet file to write merged result")
    args = parser.parse_args()
    main(args)


def test_compute_lca_strs():
    tax_list = [
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales",
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales",
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Alteromonadales"
    ]
    lineage, rank = compute_lca_strs(tax_list)
    assert lineage == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria"
    assert rank == "c"

def test_compute_lca_strs_no_common_lca():
    tax_list = [
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales",
        "d__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales",
        "d__Archaea;p__Euryarchaeota;c__Methanobacteria;o__Methanobacteriales"
    ]
    lineage, rank = compute_lca_strs(tax_list)
    assert lineage is None, f"Expected no LCA lineage, got {lineage}"
    assert rank is None, f"Expected no LCA rank, got {rank}"

def test_merge_taxonomic_info_pandas():
    df = pl.DataFrame({
        "hash": [1, 1, 2],
        "ksize": [31, 31, 31],
        "scaled": [1000, 1000, 1000],
        "dataset_names": [["A"], ["B"], ["C"]],
        "taxonomy_list": [["d__Bacteria;p__Firmicutes"], ["d__Bacteria;p__Firmicutes"], ["d__Bacteria;p__Actinobacteria"]],
        "lca_lineage": ["d__Bacteria;p__Firmicutes"] * 2 + ["d__Bacteria;p__Actinobacteria"],
        "lca_rank": ["p"] * 3,
        "source": ["db1", "db2", "db3"]
    })
    merged = merge_taxonomic_info_pandas(df)

    assert merged.height == 2
    row1 = merged.filter(pl.col("hash") == 1).to_dicts()[0]
    assert set(row1["dataset_names"]) == {"A", "B"}
    assert row1["lca_rank"] == "p"
    assert row1["lca_lineage"] == "d__Bacteria;p__Firmicutes"


def test_merge_taxonomic_info_partial_lca():
    df = pl.DataFrame({
        "hash": [3, 3],
        "ksize": [31, 31],
        "scaled": [1000, 1000],
        "dataset_names": [["X"], ["Y"]],
        "taxonomy_list": [
            ["d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales"],
            ["d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Alteromonadales"]
        ],
        "lca_lineage": [
            "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales",
            "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Alteromonadales"
        ],
        "lca_rank": ["o", "o"],
        "source": ["db1", "db2"]
    })

    merged = merge_taxonomic_info_pandas(df)

    assert merged.height == 1
    row = merged.to_dicts()[0]
    assert set(row["dataset_names"]) == {"X", "Y"}
    assert row["lca_lineage"] == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria"
    assert row["lca_rank"] == "c"
    assert row["source"] == "db1;db2"