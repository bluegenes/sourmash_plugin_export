import os
import polars as pl
import matplotlib.pyplot as plt
import argparse


RANK_ORDER = [
    "species", "genus", "family", "order", "class", "phylum", "kingdom", "domain"
]

def plot_lca_distribution(parquet_file: str, save_path: str = None):
    df = pl.read_parquet(parquet_file)

    # Drop rows without lca_rank
    df = df.filter(pl.col("lca_rank").is_not_null())

    # Count hashes per LCA rank per ksize
    rank_counts = (
        df
        .group_by(["ksize", "lca_rank"])
        .agg(pl.count("hash").alias("count"))
    )

    # Normalize to percent within each ksize
    df_norm = (
        rank_counts
        .join(
            rank_counts
            .group_by("ksize")
            .agg(pl.sum("count").alias("total")),
            on="ksize"
        )
        .with_columns([
            (pl.col("count") / pl.col("total") * 100).alias("percent")
        ])
    )

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

    bottom = [0] * len(df_pivot)
    colors = plt.get_cmap("Dark2").colors

    for idx, rank in enumerate(df_pivot.columns):
        ax.bar(df_pivot.index, df_pivot[rank], label=rank, bottom=bottom, color=colors[idx % len(colors)])
        bottom = [b + v for b, v in zip(bottom, df_pivot[rank])]

    ax.set_title("% Hashes by LCA Rank (per k-mer size)")
    ax.set_ylabel("Percentage of Hashes")
    ax.set_xlabel("k-mer size")
    ax.legend(title="LCA Rank", bbox_to_anchor=(1.05, 1), loc="upper left")
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
    parser.add_argument("parquet_file", help="Input Parquet file with LCA info and ksize values")
    parser.add_argument("--save", help="Save the plot to a file (.png, .pdf, or .svg)", default=None)
    args = parser.parse_args()
    plot_lca_distribution(args.parquet_file, args.save)
