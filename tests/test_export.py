"""
Tests for export
"""
import os
import csv
import pytest
import signal
import subprocess
import time
import polars as pl

import sourmash
from sourmash import sourmash_args
from sourmash.signature import load_one_signature_from_json
import sourmash_tst_utils as utils
from sourmash_tst_utils import SourmashCommandFailed


def get_test_data(filename):
    thisdir = os.path.dirname(__file__)
    return os.path.join(thisdir, 'test-data', filename)

def test_installed(runtmp):
    with pytest.raises(utils.SourmashCommandFailed):
        runtmp.sourmash('scripts', 'toparquet')

    assert 'usage:  toparquet' in runtmp.last_result.err


def test_rocksdb_toparquet_simple(runtmp, capfd):
    revindex = get_test_data('podar-ref-subset.branch0_9_13.internal.rocksdb')
    out_parquet = runtmp.output('podar-ref-subset.branch0_9_13.internal.parquet')

    runtmp.sourmash('scripts', 'toparquet', revindex, '--output', out_parquet)

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert os.path.exists(out_parquet), f"Expected output file at {out_parquet}."

    # Optionally verify content with Polars
    df = pl.read_parquet(out_parquet)
    # print the first few rows
    print(df.head())
    assert "hash" in df.columns
    assert "dataset_names" in df.columns
    assert len(df) == 84
    # check first few lines
    assert df[0, "hash"] == 2925290528259
    print(";".join(df[0, "dataset_names"]))
    names0 = df[0, "dataset_names"]
    assert ";".join(names0) == "NC_009661.1 Shewanella baltica OS185 plasmid pS18501, complete sequence;NC_011665.1 Shewanella baltica OS223 plasmid pS22303, complete sequence"


def test_rocksdb_toparquet_test6_no_taxonomy(runtmp, capfd):
    revindex = get_test_data('test6.rocksdb')
    out_parquet = runtmp.output('test6.parquet')

    runtmp.sourmash('scripts', 'toparquet', revindex, '--output', out_parquet)

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert os.path.exists(out_parquet), f"Expected output file at {out_parquet}."

    # verify content with Polars
    df = pl.read_parquet(out_parquet)
    # print the first few rows
    print(df.head())
    assert "hash" in df.columns
    assert "dataset_names" in df.columns
    assert len(df) == 23910
    # check there are only two columns in the file
    assert len(df.columns) == 2
    # check some lines
    assert df[0, "hash"] == 15249706293397504
    print(";".join(df[50, "dataset_names"]))
    names0 = df[50, "dataset_names"]
    assert ";".join(names0) == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"


def test_rocksdb_toparquet_test6_with_taxonomy(runtmp, capfd):
    revindex = get_test_data('test6.rocksdb')
    tax_csv = get_test_data('test6.taxonomy.csv')
    out_parquet = runtmp.output('test6.parquet')

    runtmp.sourmash('scripts', 'toparquet', revindex, '--output', out_parquet, '--taxonomy', tax_csv)

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert os.path.exists(out_parquet), f"Expected output file at {out_parquet}."

    # verify content with Polars
    df = pl.read_parquet(out_parquet)
    # print the first few rows
    print(df.head())
    assert "hash" in df.columns
    assert "dataset_names" in df.columns
    assert len(df) == 23910
    # check some lines
    assert df[0, "hash"] == 15249706293397504
    print(";".join(df[50, "dataset_names"]))
    names0 = df[50, "dataset_names"]
    assert ";".join(names0) == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"
    print(";".join(df[50, "taxonomy_list"]))
    tax_list = df[50, "taxonomy_list"]
    assert ";".join(tax_list) == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica;d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    print(df[50, "lca_lineage"])
    lca_lineage = df[50, "lca_lineage"]
    assert lca_lineage == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    print(df[50, "lca_rank"])
    lca_rank = df[50, "lca_rank"]
    assert lca_rank == "species"


def test_rocksdb_toparquet_test6_with_taxonomy_superkindom(runtmp, capfd):
    # check that superkingdom header can be used in place of domain
    revindex = get_test_data('test6.rocksdb')
    tax_csv = get_test_data('test6.taxonomy.csv')
    tax_mod = runtmp.output('test6.taxonomy-superkingdom.csv')
    out_parquet = runtmp.output('test6.parquet')

    # open tax_csv and replace "domain" in header with "superkingdom"
    with open(tax_csv, 'r') as f:
        rows = list(csv.DictReader(f))
        header = ["superkingdom" if col == "domain" else col for col in rows[0].keys()]
        updated_rows = [{("superkingdom" if k == "domain" else k): v for k, v in row.items()} for row in rows]
        with open(tax_mod, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(updated_rows)

    runtmp.sourmash('scripts', 'toparquet', revindex, '--output', out_parquet, '--taxonomy', tax_mod)

    print(runtmp.last_result.out)
    print(runtmp.last_result.err)

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert os.path.exists(out_parquet), f"Expected output file at {out_parquet}."

    # verify content with Polars
    df = pl.read_parquet(out_parquet)
    # print the first few rows
    print(df.head())
    assert "hash" in df.columns
    assert "dataset_names" in df.columns
    assert len(df) == 23910
    # check some lines
    assert df[0, "hash"] == 15249706293397504
    print(";".join(df[50, "dataset_names"]))
    names0 = df[50, "dataset_names"]
    assert ";".join(names0) == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"
    print(";".join(df[50, "taxonomy_list"]))
    tax_list = df[50, "taxonomy_list"]
    assert ";".join(tax_list) == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica;d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    print(df[50, "lca_lineage"])
    lca_lineage = df[50, "lca_lineage"]
    assert lca_lineage == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    print(df[50, "lca_rank"])
    lca_rank = df[50, "lca_rank"]
    assert lca_rank == "species"
