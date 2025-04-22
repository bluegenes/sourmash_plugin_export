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
    domain: Option<String>,
    phylum: Option<String>,
    class: Option<String>,
    order: Option<String>,
    family: Option<String>,
    genus: Option<String>,
    species: Option<String>,
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

    let revindex = match revindex {
        RevIndex::Plain(db) => db,
        // optionally handle other variants
        _ => anyhow::bail!("Expected RocksDB-based RevIndex (Plain)"),
    };
    eprintln!("DB opened");
    let db = &revindex.db;
    let cf = db.cf_handle("hashes").expect("Missing 'hashes' CF");

    // collect hash + dataset name list
    let results: Vec<(u64, Vec<String>, Vec<String>)> = db
        .iterator_cf(&cf, rocksdb::IteratorMode::Start)
        .filter_map(|res| res.ok())
        .filter_map(|(k, v)| {
            // eprintln!("Processing key: {:?}, value: {:?}", k, v);
            if k.len() != 8 {
                return None;
            }

            let hash = LittleEndian::read_u64(&k);
            // eprintln!("Hash: {hash}");

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

            let taxonomy_list: Vec<String> = dataset_names
                .iter()
                .filter_map(|name| name.split_whitespace().next()) // get the accession
                .filter_map(|accession| tax_map.as_ref().and_then(|m| m.get(accession)))
                .cloned()
                .collect();

            Some((hash, dataset_names, taxonomy_list))
        })
        .collect();

    let hashes: Vec<u64> = results.iter().map(|(h, _, _)| *h).collect();
    let dataset_lists: Vec<Option<Vec<String>>> = results
        .iter()
        .map(|(_, names, _)| Some(names.clone()))
        .collect();
    let taxonomy_lists: Vec<Option<Vec<String>>> = results
        .iter()
        .map(|(_, _, tax)| Some(tax.clone()))
        .collect();

    eprintln!("Collected {} hashes. Now making parquet.", hashes.len());

    // make dataframe
    let hash_series = Series::new("hash", hashes);
    let datasets_chunked: ListChunked = dataset_lists
        .iter()
        .map(|opt| opt.as_ref().map(|v| Series::new("", v)))
        .collect::<ListChunked>()
        .with_name("dataset_names");

    let datasets_series = datasets_chunked.into_series();

    let taxonomy_chunked: ListChunked = taxonomy_lists
        .iter()
        .map(|opt| opt.as_ref().map(|v| Series::new("", v)))
        .collect();
    let taxonomy_chunked = taxonomy_chunked.with_name("taxonomy_list");
    let taxonomy_series = taxonomy_chunked.into_series();

    let mut df = DataFrame::new(vec![hash_series, datasets_series, taxonomy_series])?;

    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;

    Ok(())
}
