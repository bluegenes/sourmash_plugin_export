#! /usr/bin/env python
import os
import sys
from sourmash.logging import notify
from sourmash.plugins import CommandLinePlugin
import importlib.metadata
import argparse

from . import sourmash_plugin_export

__version__ = importlib.metadata.version("sourmash_plugin_export")


def print_version():
    notify(f"=> sourmash_plugin_export {__version__}")


def get_max_cores():
    try:
        if "SLURM_CPUS_ON_NODE" in os.environ:
            return int(os.environ["SLURM_CPUS_ON_NODE"])
        elif "SLURM_JOB_CPUS_PER_NODE" in os.environ:
            cpus_per_node_str = os.environ["SLURM_JOB_CPUS_PER_NODE"]
            return int(cpus_per_node_str.split("x")[0])
        else:
            return os.cpu_count()
    except Exception:
        return os.cpu_count()


def set_thread_pool(user_cores):
    avail_threads = get_max_cores()
    num_threads = min(avail_threads, user_cores) if user_cores else avail_threads
    if user_cores and user_cores > avail_threads:
        notify(
            f"warning: only {avail_threads} threads available, using {avail_threads}"
        )
    actual_rayon_cores = sourmash_plugin_export.set_global_thread_pool(num_threads)
    return actual_rayon_cores


def non_negative_int(value):
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError(
            f"Batch size cannot be negative (input value: {value})"
        )
    return ivalue


class ToParquet(CommandLinePlugin):
    command = "toparquet"
    description = "export sourmash signatures (currently revindex only) to parquet"

    def __init__(self, p):
        super().__init__(p)
        p.add_argument(
            "database",
            help="A sourmash sketch database (currently revindex only).",
        )
        p.add_argument(
            "-o",
            "--output",
            help="Output file name (parquet).",
        )
        p.add_argument(
            "-t",
            "--taxonomy",
            "--lineages",
            default=None,
            help="Taxonomy CSV file (optional).",
        )
        p.add_argument(
            "-c",
            "--cores",
            default=0,
            type=int,
            help="Number of cores to use (default is all available).",
        )

    def main(self, args):
        print_version()
        
        num_threads = set_thread_pool(args.cores)

        notify(
            f"Exporting sketches in '{args.database} to parquet using {num_threads} threads."
        )

        super().main(args)
        if args.output is None:
            base = os.path.basename(args.database)
            notify(f"No output file specified, using default: '{base}.parquet'")
            args.output = f"{base}.parquet"
        status = sourmash_plugin_export.do_export_to_parquet(
            args.database,
            args.output,
            args.taxonomy,
            False,
        )

        if status == 0:
            notify("...export is done!")

        return status
