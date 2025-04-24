use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use camino::Utf8Path;
use polars::prelude::*;
use serde::Deserialize;
use sourmash::index::revindex::{Datasets, RevIndex, RevIndexOps};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Deserialize)]
struct TaxonomyRow {
    ident: String,
    // allow for both "domain" and "superkingdom" as keys
    #[serde(alias = "superkingdom")]
    domain: Option<String>,

    phylum: Option<String>,
    class: Option<String>,
    order: Option<String>,
    family: Option<String>,
    genus: Option<String>,
    species: Option<String>,
}

#[derive(Debug)]
struct HashInfo {
    hash: u64,
    dataset_names: Vec<String>,
    taxonomy_list: Option<Vec<String>>,
    lca_lineage: Option<String>,
    lca_rank: Option<&'static str>,
}

fn print_lca_summary(rank_counts: &HashMap<&str, usize>, none_count: usize, total: usize) {
    eprintln!("--- LCA Summary ---");

    let mut rank_keys: Vec<_> = rank_counts.keys().copied().collect();
    rank_keys.sort_by_key(|r| match *r {
        "domain" => 0,
        "phylum" => 1,
        "class" => 2,
        "order" => 3,
        "family" => 4,
        "genus" => 5,
        "species" => 6,
        _ => 7,
    });

    for rank in rank_keys {
        let count = rank_counts[rank];
        let pct = (count as f64 / total as f64) * 100.0;
        eprintln!("{rank}: {count} ({pct:.1}%)");
    }

    if none_count > 0 {
        let pct = (none_count as f64 / total as f64) * 100.0;
        eprintln!("unclassified: {none_count} ({pct:.1}%)");
    }
    eprintln!("Total hashes: {}", total);
    eprintln!("-------------------\n");
}

fn compute_lca_strs(taxonomies: &[String]) -> (String, Option<&'static str>) {
    if taxonomies.is_empty() {
        return (String::new(), None);
    }

    let rank_names = [
        "domain", "phylum", "class", "order", "family", "genus", "species",
    ];

    let split_taxonomies: Vec<Vec<&str>> =
        taxonomies.iter().map(|s| s.split(';').collect()).collect();

    let first = &split_taxonomies[0];
    let mut lca = Vec::new();
    let mut lca_rank = None;

    for (i, val) in first.iter().enumerate() {
        if split_taxonomies
            .iter()
            .all(|parts| parts.get(i) == Some(val))
        {
            lca.push(*val);
            lca_rank = rank_names.get(i).copied();
        } else {
            break;
        }
    }

    (lca.join(";"), lca_rank)
}

fn load_taxonomy_map(path: &Utf8Path) -> Result<HashMap<String, String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(reader);

    let mut tax_map = HashMap::new();

    for result in rdr.deserialize() {
        let row: TaxonomyRow = result?;
        let taxonomy = [
            row.domain,
            row.phylum,
            row.class,
            row.order,
            row.family,
            row.genus,
            row.species,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(";");

        tax_map.insert(row.ident, taxonomy);
    }

    if tax_map.is_empty() {
        anyhow::bail!(
            "Provided taxonomy file '{}' is empty or failed to parse.",
            path
        );
    }

    Ok(tax_map)
}

pub fn export_revindex_to_parquet(
    db_path: &Utf8Path,
    out_path: &Utf8Path,
    tax_path: Option<&Utf8Path>,
    rw: bool,
) -> Result<()> {
    println!("Opening DB (rw mode? {})", rw);
    let revindex = RevIndex::open(db_path, !rw, None)
        .map_err(|e| anyhow::anyhow!("cannot open RocksDB database. Error is: {e}"))?;

    // optionally load taxonomy
    let tax_map = if let Some(path) = tax_path {
        Some(load_taxonomy_map(path)?)
    } else {
        None
    };

    let taxonomy_enabled = tax_map.is_some();

    let RevIndex::Plain(revindex) = revindex;
    eprintln!("DB opened");
    let db = &revindex.db;
    let cf = db.cf_handle("hashes").expect("Missing 'hashes' CF");

    // collect hash + dataset name list
    let results: Vec<HashInfo> = db
        .iterator_cf(&cf, rocksdb::IteratorMode::Start)
        .filter_map(|res| res.ok())
        .filter_map(|(k, v)| {
            if k.len() != 8 {
                return None;
            }

            let hash = LittleEndian::read_u64(&k);

            let datasets = match Datasets::from_slice(&v) {
                Some(d) => d,
                None => {
                    eprintln!("Warning: could not parse dataset list");
                    return None;
                }
            };

            let dataset_names: Vec<String> = datasets
                .into_iter()
                .filter_map(|idx| {
                    if (idx as usize) >= revindex.collection().len() {
                        eprintln!("Skipping invalid dataset ID: {}", idx);
                        return None;
                    }
                    let record = revindex.collection().record_for_dataset(idx).ok()?;
                    let name = record.name();
                    if !name.is_empty() {
                        Some(name.to_string())
                    } else {
                        Some(record.filename().to_string())
                    }
                })
                .collect();

            if let Some(tax_map) = &tax_map {
                let taxonomy_list: Vec<String> = dataset_names
                    .iter()
                    .filter_map(|name| name.split_whitespace().next())
                    .filter_map(|accession| tax_map.get(accession))
                    .cloned()
                    .collect();
                let (lca_lineage, lca_rank) = compute_lca_strs(&taxonomy_list);
                Some(HashInfo {
                    hash,
                    dataset_names,
                    taxonomy_list: Some(taxonomy_list),
                    lca_lineage: Some(lca_lineage),
                    lca_rank,
                })
            } else {
                Some(HashInfo {
                    hash,
                    dataset_names,
                    taxonomy_list: None,
                    lca_lineage: None,
                    lca_rank: None,
                })
            }
        })
        .collect();

    let n_rows = results.len();
    let mut hashes = Vec::with_capacity(n_rows);
    let mut dataset_lists = Vec::with_capacity(n_rows);

    let (mut taxonomy_lists, mut lca_lineages, mut lca_ranks): (Option<_>, Option<_>, Option<_>) =
        if taxonomy_enabled {
            (
                Some(Vec::with_capacity(results.len())),
                Some(Vec::with_capacity(results.len())),
                Some(Vec::with_capacity(results.len())),
            )
        } else {
            (None, None, None)
        };

    let mut rank_counts: HashMap<&str, usize> = HashMap::new();
    let mut none_count = 0;

    for result in results {
        hashes.push(result.hash);
        dataset_lists.push(Some(result.dataset_names));

        if taxonomy_enabled {
            taxonomy_lists.as_mut().unwrap().push(result.taxonomy_list);
            lca_lineages
                .as_mut()
                .unwrap()
                .push(result.lca_lineage.unwrap_or_default());
            lca_ranks.as_mut().unwrap().push(result.lca_rank);

            match result.lca_rank {
                Some(r) => *rank_counts.entry(r).or_insert(0) += 1,
                None => none_count += 1,
            }
        }
    }

    eprintln!("Collected {} hashes. Now making parquet.", hashes.len());

    // make dataframe
    let hash_series = Series::new("hash", hashes);
    let datasets_chunked: ListChunked = dataset_lists
        .iter()
        .map(|opt| opt.as_ref().map(|v| Series::new("", v)))
        .collect::<ListChunked>()
        .with_name("dataset_names");

    let datasets_series = datasets_chunked.into_series();

    let mut df_columns = vec![hash_series, datasets_series];

    if taxonomy_enabled {
        let taxonomy_chunked: ListChunked = taxonomy_lists
            .unwrap()
            .iter()
            .map(|opt| opt.as_ref().map(|v| Series::new("", v)))
            .collect();
        let taxonomy_chunked = taxonomy_chunked.with_name("taxonomy_list");
        let taxonomy_series = taxonomy_chunked.into_series();

        let lca_lineage_series = Series::new("lca_lineage", lca_lineages.unwrap());
        let lca_rank_series = Series::new("lca_rank", lca_ranks.unwrap());

        df_columns.push(taxonomy_series);
        df_columns.push(lca_lineage_series);
        df_columns.push(lca_rank_series);
    }

    let df = DataFrame::new(df_columns)?;
    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;

    eprintln!("Exported to '{}'\n", out_path);
    if tax_path.is_some() {
        print_lca_summary(&rank_counts, none_count, n_rows);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_lineages() {
        let input = vec![
            "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica".to_string(),
            "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Shewanellaceae;g__Shewanella;s__Shewanella baltica".to_string(),
        ];
        let (lca, rank) = compute_lca_strs(&input);
        assert_eq!(lca, input[0]);
        assert_eq!(rank, Some("species"));
    }

    #[test]
    fn test_partial_lca() {
        let input = vec![
            "d__Bacteria;p__Bacteroidota;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae;g__Phocaeicola;s__Phocaeicola vulgatus".to_string(),
            "d__Bacteria;p__Bacteroidota;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae;g__Prevotella;s__Prevotella copri_B".to_string(),
        ];
        let (lca, rank) = compute_lca_strs(&input);
        assert_eq!(
            lca,
            "d__Bacteria;p__Bacteroidota;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae"
        );
        assert_eq!(rank, Some("family"));
    }

    #[test]
    fn test_no_common_lca() {
        let input = vec![
            "d__Bacteria;p__Firmicutes".to_string(),
            "d__Archaea;p__Euryarchaeota".to_string(),
        ];
        let (lca, rank) = compute_lca_strs(&input);
        assert_eq!(lca, "");
        assert_eq!(rank, None);
    }

    #[test]
    fn test_single_entry() {
        let input = vec!["d__Bacteria;p__Firmicutes;c__Bacilli".to_string()];
        let (lca, rank) = compute_lca_strs(&input);
        assert_eq!(lca, input[0]);
        assert_eq!(rank, Some("class"));
    }

    #[test]
    fn test_empty_input() {
        let input: Vec<String> = vec![];
        let (lca, rank) = compute_lca_strs(&input);
        assert_eq!(lca, "");
        assert_eq!(rank, None);
    }
}
