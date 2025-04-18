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


def test_toparquet_simple(runtmp, capfd):
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
