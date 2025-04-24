"""
Tests for export
"""

import os
import csv
import pytest
import polars as pl
import pandas as pd

import sourmash_tst_utils as utils
from sourmash_tst_utils import SourmashCommandFailed

def get_test_data(filename):
    thisdir = os.path.dirname(__file__)
    return os.path.join(thisdir, "test-data", filename)


def test_installed(runtmp):
    with pytest.raises(utils.SourmashCommandFailed):
        runtmp.sourmash("scripts", "revindex_to_parquet")

    assert "usage:  revindex_to_parquet" in runtmp.last_result.err


def test_rocksdb_revindex_to_parquet_simple(runtmp, capfd):
    revindex = get_test_data("podar-ref-subset.branch0_9_13.internal.rocksdb")
    out_parquet = runtmp.output("podar-ref-subset.branch0_9_13.internal.parquet")

    runtmp.sourmash("scripts", "revindex_to_parquet", revindex, "--output", out_parquet)

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
    assert (
        ";".join(names0)
        == "NC_009661.1 Shewanella baltica OS185 plasmid pS18501, complete sequence;NC_011665.1 Shewanella baltica OS223 plasmid pS22303, complete sequence"
    )


def test_rocksdb_revindex_to_parquet_test6_no_taxonomy(runtmp, capfd):
    revindex = get_test_data("test6.rocksdb")
    out_parquet = runtmp.output("test6.parquet")

    runtmp.sourmash("scripts", "revindex_to_parquet", revindex, "--output", out_parquet)

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
    # check that all columns are present 
    assert len(df.columns) == 8
    # check some lines
    assert df[0, "hash"] == 15249706293397504
    assert df[50, "hash"] == 8357480319128064
    print(";".join(df[50, "dataset_names"]))
    names0 = df[50, "dataset_names"]
    print(names0)
    assert (
        ";".join(names0)
        == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"
    )


def test_rocksdb_revindex_to_parquet_test6_with_taxonomy(runtmp, capfd):
    revindex = get_test_data("test6.rocksdb")
    tax_csv = get_test_data("test6.taxonomy.csv")
    out_parquet = runtmp.output("test6.parquet")
    out_lca = runtmp.output("test6.lca.csv")

    runtmp.sourmash(
        "scripts", "revindex_to_parquet", revindex, "--output", out_parquet, "--taxonomy", tax_csv, "--lca-info", out_lca
    )

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
    assert (
        ";".join(names0)
        == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"
    )
    print(";".join(df[50, "taxonomy_list"]))
    tax_list = df[50, "taxonomy_list"]
    assert (
        ";".join(tax_list)
        == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica;d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    )
    print(df[50, "lca_lineage"])
    lca_lineage = df[50, "lca_lineage"]
    assert (
        lca_lineage
        == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    )
    print(df[50, "lca_rank"])
    lca_rank = df[50, "lca_rank"]
    assert lca_rank == "species"

    # check that the lca file was created
    assert os.path.exists(out_lca), f"Expected output file at {out_lca}."
    # check that the lca file contains the expected columns
    lca_df = pd.read_csv(out_lca)
    print(lca_df)
    expected_rows = [
        {"source": "test6.rocksdb", "rank": "family", "count": 8, "percent": 0.03},
        {"source": "test6.rocksdb", "rank": "species", "count": 23901, "percent": 99.96},
        {"source": "test6.rocksdb", "rank": "order", "count": 1, "percent": 0.00},
        {"source": "combined", "rank": "family", "count": 8, "percent": 0.03},
        {"source": "combined", "rank": "order", "count": 1, "percent": 0.00},
        {"source": "combined", "rank": "species", "count": 23901, "percent": 99.96},
    ]

    for expected_row in expected_rows:
        assert any(
            all(row[k] == expected_row[k] for k in expected_row)
            for row in lca_df.to_dict(orient="records")
        ), f"Expected row not found: {expected_row}"



def test_rocksdb_revindex_to_parquet_test6_with_taxonomy_superkindom(runtmp, capfd):
    # check that superkingdom header can be used in place of domain
    revindex = get_test_data("test6.rocksdb")
    tax_csv = get_test_data("test6.taxonomy.csv")
    tax_mod = runtmp.output("test6.taxonomy-superkingdom.csv")
    out_parquet = runtmp.output("test6.parquet")

    # open tax_csv and replace "domain" in header with "superkingdom"
    with open(tax_csv, "r") as f:
        rows = list(csv.DictReader(f))
        header = ["superkingdom" if col == "domain" else col for col in rows[0].keys()]
        updated_rows = [
            {("superkingdom" if k == "domain" else k): v for k, v in row.items()}
            for row in rows
        ]
        with open(tax_mod, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(updated_rows)

    runtmp.sourmash(
        "scripts", "revindex_to_parquet", revindex, "--output", out_parquet, "--taxonomy", tax_mod
    )

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
    assert (
        ";".join(names0)
        == "GCF_000021665.1 Shewanella baltica OS223;GCF_000017325.1 Shewanella baltica OS185"
    )
    print(";".join(df[50, "taxonomy_list"]))
    tax_list = df[50, "taxonomy_list"]
    assert (
        ";".join(tax_list)
        == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica;d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    )
    print(df[50, "lca_lineage"])
    lca_lineage = df[50, "lca_lineage"]
    assert (
        lca_lineage
        == "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica"
    )
    print(df[50, "lca_rank"])
    lca_rank = df[50, "lca_rank"]
    assert lca_rank == "species"


def test_rocksdb_revindex_to_parquet_test6_bad_tax_file(runtmp, capfd):
    revindex = get_test_data("test6.rocksdb")
    tax_csv = get_test_data("test6.gbsketch.csv")
    out_parquet = runtmp.output("test6.parquet")

    with pytest.raises(SourmashCommandFailed):
        runtmp.sourmash(
            "scripts",
            "revindex_to_parquet",
            revindex,
            "--output",
            out_parquet,
            "--taxonomy",
            tax_csv,
        )

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert (
        "Error: CSV deserialize error: record 1 (line: 2, byte: 15): missing field `ident`"
        in captured.err
    )


def test_rocksdb_revindex_to_parquet_test6_tax_file_doesnt_exist(runtmp, capfd):
    revindex = get_test_data("test6.rocksdb")
    tax_csv = runtmp.output("test6.empty.csv")
    out_parquet = runtmp.output("test6.parquet")

    with pytest.raises(SourmashCommandFailed):
        runtmp.sourmash(
            "scripts",
            "revindex_to_parquet",
            revindex,
            "--output",
            out_parquet,
            "--taxonomy",
            tax_csv,
        )

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert "Error: No such file or directory (os error 2)" in captured.err


def test_rocksdb_revindex_to_parquet_test6_empty_tax_file(runtmp, capfd):
    revindex = get_test_data("test6.rocksdb")
    tax_csv = runtmp.output("test6.empty.csv")
    out_parquet = runtmp.output("test6.parquet")

    # make an empty file
    with open(tax_csv, "w") as f:
        pass
    # check that the file is empty
    with open(tax_csv, "r") as f:
        content = f.read()
        assert content == ""

    with pytest.raises(SourmashCommandFailed):
        runtmp.sourmash(
            "scripts",
            "revindex_to_parquet",
            revindex,
            "--output",
            out_parquet,
            "--taxonomy",
            tax_csv,
        )

    captured = capfd.readouterr()
    print(captured.out)
    print(captured.err)

    assert f"Error: Provided taxonomy file '{tax_csv}' is empty or failed to parse."


def test_rocksdb_revindex_to_parquet_test6_multiple_revindex(runtmp, capfd):
    revindex1 = get_test_data("test6.rocksdb")
    revindex2 = get_test_data("podar-ref-subset.branch0_9_13.internal.rocksdb")
    tax_csv = get_test_data("test6.taxonomy.csv")
    out_parquet = runtmp.output("test6.parquet")
    lca_csv = runtmp.output("test6.lca.csv")

    runtmp.sourmash(
        "scripts", "revindex_to_parquet", revindex1, revindex2, "--output", out_parquet, "--taxonomy", tax_csv, "--lca-info", lca_csv
    )

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
    assert len(df.columns) == 8
    assert len(df) == 23994
    # check some hashes
    target_hash_1 = 8357480319128064
    expected_names_1 = {
        "GCF_000021665.1 Shewanella baltica OS223",
        "GCF_000017325.1 Shewanella baltica OS185",
    }
    expected_tax_1 = (
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;"
        "o__Enterobacterales;f__Shewanellaceae;g__Shewanella;"
        "s__Shewanella baltica;" * 2
    ).rstrip(";")
    expected_lca_lineage_1 = (
        "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;"
        "o__Enterobacterales;f__Shewanellaceae;g__Shewanella;"
        "s__Shewanella baltica"
    )

    target_hash_2 = 2925290528259
    expected_names_2 = {
        "NC_009661.1 Shewanella baltica OS185 plasmid pS18501, complete sequence",
        "NC_011665.1 Shewanella baltica OS223 plasmid pS22303, complete sequence"
    }

    found_1 = False
    found_2 = False
    found_3 = False

    for row in df.iter_rows(named=True):
        if row["hash"] == target_hash_1:
            print(row)
            found_1 = True
            assert set(row["dataset_names"]) == expected_names_1
            assert ";".join(row["taxonomy_list"]) == expected_tax_1
            assert row["lca_lineage"] == expected_lca_lineage_1
            assert row["lca_rank"] == "species"
        elif row["hash"] == target_hash_2:
            print(row)
            # this hash is actually found in both revindexes
            if row["source"] == "podar-ref-subset.branch0_9_13.internal.rocksdb":
                found_2 = True
                assert set(row["dataset_names"]) == expected_names_2
                assert not row["taxonomy_list"], "Expected taxonomy_list to be empty or None"
                assert row["lca_lineage"] in (None, ""), "Expected no lca_lineage"
                assert row["lca_rank"] in (None, ""), "Expected no lca_rank"
                assert row["scaled"] == 100000
            elif row["source"] == "test6.rocksdb":
                found_3 = True
                assert set(row["dataset_names"]) == expected_names_1
                assert row["scaled"] == 1000
                assert ';'.join(row["taxonomy_list"]) == expected_tax_1

    assert found_1, f"Did not find a row with hash {target_hash_1}"
    assert found_2, f"Did not find a row with hash {target_hash_2} in podar-ref-subset.branch0_9_13.internal.rocksdb"
    assert found_3, f"Did not find a row with hash {target_hash_2} in test6.rocksdb"

    # check that the lca file was created
    assert os.path.exists(lca_csv), f"Expected output file at {lca_csv}."
    # check that the lca file contains the expected columns
    lca_df = pd.read_csv(lca_csv)
    print(lca_df)
    expected_rows = [
        {"source": "test6.rocksdb", "rank": "family", "count": 8, "percent": 0.03},
        {"source": "test6.rocksdb", "rank": "species", "count": 23901, "percent": 99.96},
        {"source": "test6.rocksdb", "rank": "order", "count": 1, "percent": 0.00},
        {"source": "podar-ref-subset.branch0_9_13.internal.rocksdb", "rank": "unclassified", "count": 84, "percent": 100.00},
        {"source": "combined", "rank": "family", "count": 8, "percent": 0.03},
        {"source": "combined", "rank": "order", "count": 1, "percent": 0.00},
        {"source": "combined", "rank": "species", "count": 23901, "percent": 99.61},
        {"source": "combined", "rank": "unclassified", "count": 84, "percent": 0.35},
    ]
    for expected_row in expected_rows:
        assert any(
            all(row[k] == expected_row[k] for k in expected_row)
            for row in lca_df.to_dict(orient="records")
        ), f"Expected row not found: {expected_row}"
