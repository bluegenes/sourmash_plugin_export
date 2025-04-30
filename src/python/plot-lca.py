import os
import polars as pl
import matplotlib.pyplot as plt
import argparse


RANK_ORDER = [
    "species", "genus", "family", "order", "class", "phylum", "kingdom", "domain", "no_lca"
]

# manually map colors to ranks
colors = list(plt.get_cmap("Dark2").colors)
RANK_COLORS = {
    rank: colors[idx % len(colors)] for idx, rank in enumerate(RANK_ORDER)
}
RANK_COLORS["no_lca"] = "#dddddd"  # light gray

def read_lca_summary_csvs(csv_files: list[str]) -> pl.DataFrame:
    dfs = []
    for f in csv_files:
        try:
            df = pl.read_csv(f)
            # keep only required columns
            dfs.append(df.select(["ksize", "lca_rank", "percent"]))
        except Exception as e:
            print(f"Warning: failed to read summary CSV '{f}': {e}")
    if not dfs:
        raise ValueError("No valid LCA summary CSVs were read.")
    return pl.concat(dfs)

def read_parquet_files(parquet_files: list[str]) -> pl.DataFrame:
    dfs = []
    for f in parquet_files:
        try:
            df = pl.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"Warning: failed to read Parquet file '{f}': {e}")
    if not dfs:
        raise ValueError("No valid Parquet files were read.")
    df = pl.concat(dfs)

    # Drop "unclassified" rows, but keep "no_lca" rows
    df = (
        df
        .filter(~pl.col("lca_rank").eq("unclassified"))  # drop true "unclassified"
        .with_columns([
            pl.when(pl.col("lca_rank").is_null())
            .then("no_lca")
            .otherwise(pl.col("lca_rank"))
            .alias("lca_rank")
        ])
    )

    # Count hashes per LCA rank per ksize
    rank_counts = df.group_by(["ksize", "lca_rank"]).agg(pl.count("hash").alias("count"))

    # Normalize to percent within each ksize
    df_norm = (
        rank_counts.join(
            rank_counts.group_by("ksize").agg(pl.sum("count").alias("total")),
            on="ksize"
        ).with_columns(
            (pl.col("count") / pl.col("total") * 100).alias("percent")
        )
    )
    return df_norm.select(["ksize", "lca_rank", "percent"])

def plot_lca_distribution(input_files: list[str], save_path: str = None):
    csv_files = [f for f in input_files if f.endswith(".csv")]
    parquet_files = [f for f in input_files if f.endswith(".parquet")]

    if csv_files and parquet_files:
        raise ValueError("Please provide only .csv or only .parquet files, not both.")

    if csv_files:
        df_norm = read_lca_summary_csvs(csv_files)
    elif parquet_files:
        df_norm = read_parquet_files(parquet_files)
    else:
        raise ValueError("No valid .csv or .parquet files provided.")

    # Convert to Pandas for pivot and plot
    df_pandas = (
        df_norm
        .with_columns(pl.col("ksize").cast(pl.Utf8))
        .select(["ksize", "lca_rank", "percent"])
        .to_pandas()
    )

    # Step 2: Pivot for stacked bar
    df_pivot = df_pandas.pivot_table(
        index="ksize", columns="lca_rank", values="percent", aggfunc="sum", fill_value=0
    )
    df_pivot = df_pivot.reindex(columns=RANK_ORDER, fill_value=0)

    # Step 3: Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    legend_elements = []

    bottom = [0] * len(df_pivot)
    for rank in RANK_ORDER:
        if rank in df_pivot.columns:
            bar = ax.bar(
                df_pivot.index,
                df_pivot[rank],
                label=rank,
                bottom=bottom,
                color=RANK_COLORS.get(rank, "#333333")  # fallback to dark gray if needed
            )
            bottom = [b + v for b, v in zip(bottom, df_pivot[rank])]
            
            # Save just one artist for the legend (not one per bar group)
            legend_elements.append((rank, bar[0]))

    if args.title_basename:
        title = f"{args.title_basename} - % Hashes by LCA Rank (per k-mer size)"
    else:
        title = "% Hashes by LCA Rank (per k-mer size)"
    ax.set_title(title)
    ax.set_ylabel("Percentage of Hashes")
    ax.set_xlabel("k-mer size")
    labels, handles = zip(*legend_elements[::-1])
    ax.legend(handles, labels, title="LCA Rank", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()

    if save_path:
        ext = os.path.splitext(save_path)[1].lower()
        if ext not in {".png", ".pdf", ".svg"}:
            raise ValueError("Unsupported file extension. Use .png, .pdf, or .svg")
        plt.savefig(save_path, dpi=300)
        print(f"Plot saved to {save_path}")
    else:
        plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot % hashes by LCA rank per k-mer size.")
    parser.add_argument("input_files", nargs="+", help="One or more input parquet files or lca summary files")
    parser.add_argument("--save", help="Save the plot to a file (.png, .pdf, or .svg)", default=None)
    parser.add_argument("--title-basename", help="title basename for the plot", default=None)
    args = parser.parse_args()
    plot_lca_distribution(args.input_files, args.save)

