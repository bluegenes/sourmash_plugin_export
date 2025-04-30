use anyhow::{anyhow, Result};
use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result as ArrowResult;
use arrow2::io::parquet::write::CompressionOptions;
use arrow2::io::parquet::write::*;
use arrow2::offset::{Offsets, OffsetsBuffer};
use byteorder::{ByteOrder, LittleEndian};
use camino::{Utf8Path, Utf8PathBuf};
use csv::Writer;
use rayon::prelude::*;
use serde::Deserialize;
use sourmash::index::revindex::{Datasets, RevIndex, RevIndexOps};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

fn setup_ctrlc_handler(cancel_flag: Arc<AtomicBool>) -> Result<()> {
    if std::env::var("PYTEST_RUNNING").is_err() {
        let cancel_flag = cancel_flag.clone();
        ctrlc::set_handler(move || {
            eprintln!("Received Ctrl-C! Will terminate after current item...");
            cancel_flag.store(true, Ordering::SeqCst);
        })?;
    } else {
        eprintln!("Running under pytest, skipping Ctrl-C handler setup.");
    }
    Ok(())
}

// Record struct for parquet file output
#[derive(Debug)]
struct ArrowRecord {
    hash: u64,
    dataset_names: Vec<String>,
    taxonomy_list: Option<Vec<String>>,
    lca_lineage: Option<String>,
    lca_rank: Option<String>,
    ksize: u32,
    scaled: u32,
    source: String, // basename of revindex
}

// schema for parquet file
fn create_schema() -> Schema {
    Schema::from(vec![
        Field::new("hash", DataType::UInt64, false),
        Field::new(
            "dataset_names",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
        Field::new(
            "taxonomy_list",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("lca_lineage", DataType::Utf8, true),
        Field::new("lca_rank", DataType::Utf8, true),
        Field::new("ksize", DataType::UInt32, false),
        Field::new("scaled", DataType::UInt32, false),
        Field::new("source", DataType::Utf8, false),
    ])
}

fn string_list_array(values: &[Vec<String>]) -> Result<ListArray<i32>, arrow2::error::Error> {
    let flat: Vec<&str> = values.iter().flatten().map(String::as_str).collect();

    let mut offsets = Offsets::<i32>::new();
    // offsets.try_extend(values.iter().map(|v| v.len()))?;
    for v in values {
        offsets.try_push(v.len().try_into().expect("len exceeds i32::MAX"))?;
    }

    let values_array = Utf8Array::<i32>::from_slice(flat);

    Ok(ListArray::<i32>::new(
        DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
        OffsetsBuffer::from(offsets),
        Box::new(values_array),
        None,
    ))
}

/// Returns: schema and chunk (i.e., row group)
fn convert_to_batch(records: &[ArrowRecord]) -> ArrowResult<(Schema, Chunk<Box<dyn Array>>)> {
    let hashes = UInt64Array::from_slice(records.iter().map(|r| r.hash).collect::<Vec<_>>());
    let ksizes = UInt32Array::from_slice(records.iter().map(|r| r.ksize).collect::<Vec<_>>());
    let scaleds = UInt32Array::from_slice(records.iter().map(|r| r.scaled).collect::<Vec<_>>());
    let source = Utf8Array::<i32>::from_slice(
        records
            .iter()
            .map(|r| r.source.as_str())
            .collect::<Vec<_>>(),
    );

    let dataset_names = string_list_array(
        &records
            .iter()
            .map(|r| r.dataset_names.clone())
            .collect::<Vec<_>>(),
    );
    let taxonomy_list = string_list_array(
        &records
            .iter()
            .map(|r| r.taxonomy_list.clone().unwrap_or_default())
            .collect::<Vec<_>>(),
    );
    let lca_lineage = Utf8Array::<i32>::from(
        records
            .iter()
            .map(|r| r.lca_lineage.as_deref())
            .collect::<Vec<_>>(),
    );
    let lca_rank = Utf8Array::<i32>::from(
        records
            .iter()
            .map(|r| r.lca_rank.as_deref())
            .collect::<Vec<_>>(),
    );

    let chunk = Chunk::new(vec![
        Box::new(hashes) as Box<dyn Array>,
        Box::new(dataset_names?) as Box<dyn Array>,
        Box::new(taxonomy_list?) as Box<dyn Array>,
        Box::new(lca_lineage) as Box<dyn Array>,
        Box::new(lca_rank) as Box<dyn Array>,
        Box::new(ksizes) as Box<dyn Array>,
        Box::new(scaleds) as Box<dyn Array>,
        Box::new(source) as Box<dyn Array>,
    ]);

    Ok((create_schema(), chunk))
}

/// Start an MPSC writer thread that receives ArrowRecords and writes batches to a Parquet file.
/// Returns a Sender that can be cloned for use with Rayon threads.
fn start_arrow_writer_thread(
    parquet_path: Utf8PathBuf,
    flush_threshold: usize,
) -> Result<(Sender<ArrowRecord>, thread::JoinHandle<Result<()>>)> {
    let (sender, receiver): (Sender<ArrowRecord>, Receiver<ArrowRecord>) = mpsc::channel();

    let parquet_path = parquet_path.to_string();

    let handle = thread::spawn(move || -> Result<()> {
        let file = File::create(&parquet_path)?;
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let mut buffer = Vec::with_capacity(flush_threshold);

        // Prime schema from empty batch
        let (schema, _) = convert_to_batch(&[])?;
        let mut writer = FileWriter::try_new(file, schema.clone(), options)?;

        for record in receiver {
            buffer.push(record);

            if buffer.len() >= flush_threshold {
                let (_, chunk) = convert_to_batch(&buffer)?;
                let encodings = vec![vec![Encoding::Plain]; schema.fields.len()];
                let row_groups = RowGroupIterator::try_new(
                    std::iter::once(Ok(chunk)),
                    &schema,
                    options,
                    encodings,
                )?;

                for group in row_groups {
                    writer.write(group?)?;
                }

                buffer.clear();
            }
        }

        // Flush remaining records
        if !buffer.is_empty() {
            let encodings = vec![vec![Encoding::Plain]; schema.fields.len()];
            let (_, chunk) = convert_to_batch(&buffer)?;
            let row_groups =
                RowGroupIterator::try_new(std::iter::once(Ok(chunk)), &schema, options, encodings)?;

            for group in row_groups {
                writer.write(group?)?;
            }
        }

        writer.end(None)?;
        eprintln!("Finished writing Parquet to {parquet_path}");
        Ok(())
    });

    Ok((sender, handle))
}

// LCA and Taxonomy Utils
#[derive(Default, Clone)]
struct LCASummary {
    rank_counts: HashMap<String, usize>,
    no_lca_count: usize,
    unclassified_count: usize,
    total: usize,
    ksize: u32,
    scaled: u32,
}

impl LCASummary {
    pub fn new(ksize: u32, scaled: u32) -> Self {
        Self {
            ksize,
            scaled,
            ..Default::default()
        }
    }

    fn add_rank(&mut self, taxonomy_list: Option<&Vec<String>>, lca_rank: Option<&str>) {
        self.total += 1;

        match taxonomy_list {
            Some(list) if list.is_empty() => {
                self.unclassified_count += 1;
            }
            Some(_) => {
                if let Some(r) = lca_rank {
                    *self.rank_counts.entry(r.to_string()).or_default() += 1;
                } else {
                    self.no_lca_count += 1;
                }
            }
            None => {
                self.unclassified_count += 1;
            }
        }
    }

    fn merge(&mut self, other: &LCASummary) {
        for (rank, count) in &other.rank_counts {
            *self.rank_counts.entry(rank.clone()).or_default() += count;
        }
        self.unclassified_count += other.unclassified_count;
        self.no_lca_count += other.no_lca_count;
        self.total += other.total;
    }

    fn to_csv_rows(&self, source: &str) -> Vec<(String, u32, u32, String, usize, f64)> {
        let mut rows = self
            .rank_counts
            .iter()
            .map(|(rank, count)| {
                (
                    rank.clone(),
                    *count,
                    (*count as f64 / self.total as f64) * 100.0,
                )
            })
            .collect::<Vec<_>>();

        if self.no_lca_count > 0 {
            rows.push((
                "no_lca".into(),
                self.no_lca_count,
                (self.no_lca_count as f64 / self.total as f64) * 100.0,
            ));
        }

        if self.unclassified_count > 0 {
            rows.push((
                "unclassified".into(),
                self.unclassified_count,
                (self.unclassified_count as f64 / self.total as f64) * 100.0,
            ));
        }

        rows.into_iter()
            .map(|(rank, count, pct)| {
                (
                    source.to_string(),
                    self.ksize,
                    self.scaled,
                    rank,
                    count,
                    pct,
                )
            })
            .collect()
    }

    pub const CSV_HEADER: [&'static str; 6] =
        ["source", "ksize", "scaled", "lca_rank", "count", "percent"];

    pub fn write_csv<W: std::io::Write>(
        &self,
        writer: &mut csv::Writer<W>,
        source: &str,
    ) -> Result<()> {
        for (src, ksize, scaled, rank, count, pct) in self.to_csv_rows(source) {
            writer.serialize((src, ksize, scaled, rank, count, format!("{:.2}", pct)))?;
        }
        Ok(())
    }
}

impl fmt::Display for LCASummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "--- LCA Summary ---")?;

        let mut rank_keys: Vec<_> = self.rank_counts.keys().cloned().collect();
        rank_keys.sort_by_key(|r| match r.as_str() {
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
            let count = self.rank_counts[&*rank];
            let pct = (count as f64 / self.total as f64) * 100.0;
            writeln!(f, "{rank}: {count} ({pct:.1}%)")?;
        }

        if self.no_lca_count > 0 {
            let pct = (self.no_lca_count as f64 / self.total as f64) * 100.0;
            writeln!(f, "no_lca: {} ({:.1}%)", self.no_lca_count, pct)?;
        }

        if self.unclassified_count > 0 {
            let pct = (self.unclassified_count as f64 / self.total as f64) * 100.0;
            writeln!(f, "unclassified: {} ({:.1}%)", self.unclassified_count, pct)?;
        }

        writeln!(f, "Total hashes: {}", self.total)?;
        writeln!(f, "-------------------")
    }
}

fn write_lca_info(path: Option<&Utf8Path>, all_summaries: &[(String, LCASummary)]) -> Result<()> {
    if all_summaries.is_empty() {
        return Ok(()); // no summaries to print or write
    }

    // group summaries by ksize and scaled
    let mut grouped: BTreeMap<(u32, u32), Vec<(String, LCASummary)>> = BTreeMap::new();
    for (source, summary) in all_summaries {
        grouped
            .entry((summary.ksize, summary.scaled))
            .or_default()
            .push((source.clone(), summary.clone()));
    }

    // Open writer if writing CSV
    let mut wtr = path.map(Writer::from_path).transpose()?;
    if let Some(ref mut writer) = wtr {
        writer.write_record(LCASummary::CSV_HEADER)?;
    }

    // first, just iterate + write individual summaries
    // bc always want these first
    for group in grouped.values() {
        for (source, summary) in group {
            eprintln!("{source}:");
            eprintln!("{summary}");
            if let Some(ref mut writer) = wtr {
                summary.write_csv(writer, source)?;
            }
        }
    }

    // now iterate over grouped summaries, combining them and printing/writing
    for ((ksize, scaled), group) in &grouped {
        if group.len() <= 1 {
            continue;
        }

        let mut combined = LCASummary::new(*ksize, *scaled);
        for (_, summary) in group {
            combined.merge(summary);
        }

        let label = format!("combined_k{ksize}_sc{scaled}");
        eprintln!("Combined summary for k={ksize}, scaled={scaled}:");
        eprintln!("{combined}");

        if let Some(ref mut writer) = wtr {
            combined.write_csv(writer, &label)?;
        }
    }

    if let Some(ref mut writer) = wtr {
        writer.flush()?;
    }

    Ok(())
}

fn strip_accession_version(s: &str) -> &str {
    s.split('.').next().unwrap_or(s)
}

#[derive(Debug, Deserialize)]
struct TaxonomyRow {
    #[serde(alias = "identifier", alias = "identifier", alias = "accession")]
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

fn load_taxonomy_map(path: Utf8PathBuf) -> Result<HashMap<String, String>> {
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(reader);

    let mut tax_map = HashMap::new();
    let mut total_rows = 0;
    let mut failed_rows = 0;

    for result in rdr.deserialize() {
        total_rows += 1;
        match result {
            Ok(row) => {
                let row: TaxonomyRow = row;

                // Check if at least one taxonomy field is present
                let has_taxonomy = row.domain.is_some()
                    || row.phylum.is_some()
                    || row.class.is_some()
                    || row.order.is_some()
                    || row.family.is_some()
                    || row.genus.is_some()
                    || row.species.is_some();

                if !has_taxonomy {
                    failed_rows += 1;
                    eprintln!(
                        "Warning: row {} has no taxonomy fields, skipping accession '{}'.",
                        total_rows, row.ident
                    );
                    continue;
                }

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
                let ident = strip_accession_version(&row.ident);
                tax_map.insert(ident.to_string(), taxonomy);
            }
            Err(e) => {
                failed_rows += 1;
                eprintln!("Warning: failed to parse row {}: {}", total_rows, e);
            }
        }
    }

    if tax_map.is_empty() {
        anyhow::bail!(
            "Provided taxonomy file '{}' is empty or failed to parse.",
            path
        );
    }

    eprintln!(
        "Loaded {} taxonomy entries ({} rows failed to parse).",
        tax_map.len(),
        failed_rows
    );

    Ok(tax_map)
}

// process single revindex
fn process_revindex(
    db_path: &Utf8Path,
    sender: &Sender<ArrowRecord>,
    taxonomy_map: Option<&HashMap<String, String>>,
    rw: bool,
    cancel_flag: Arc<AtomicBool>,
) -> Result<LCASummary> {
    // get basename of revindex directory for us to write later
    let db_basename = db_path
        .file_name()
        .ok_or_else(|| anyhow!("Cannot get basename of path: {}", db_path))?
        .to_string();
    println!("Opening DB (rw mode? {})", rw);
    let revindex = RevIndex::open(db_path, !rw, None)
        .map_err(|e| anyhow::anyhow!("cannot open RocksDB database. Error is: {e}"))?;

    let RevIndex::Plain(revindex) = revindex;
    eprintln!("DB opened");

    // get ksize, scaled from the first record
    // (assuming all records in a RocksDB have the same ksize and scaled)
    let manifest = revindex.collection().manifest();
    let (ksize, scaled) = manifest
        .iter()
        .next()
        .map(|record| (record.ksize(), record.scaled()))
        .ok_or_else(|| anyhow!("No records in manifest"))?;

    let db = &revindex.db;
    let cf = db.cf_handle("hashes").expect("Missing 'hashes' CF");

    // estimate total hashes to process
    // this is not exact, but should be close enough
    let total_hashes = db
        .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")?
        .ok_or_else(|| anyhow!("Could not get estimated number of hashes"))?;

    eprintln!(
        "Estimated total hashes to process for {}: {}",
        db_path, total_hashes
    );

    let mut lca_summary = LCASummary::new(ksize, *scaled);
    let mut processed = 0;
    let mut next_percent = 1;
    eprintln!("Iterating across hashes...");

    for (k, v) in db
        .iterator_cf(&cf, rocksdb::IteratorMode::Start)
        .filter_map(Result::ok)
    {
        // Check for cancellation
        if cancel_flag.load(Ordering::SeqCst) {
            eprintln!("Cancellation detected! Stopping iteration.");
            break;
        }

        if k.len() != 8 {
            continue;
        }

        processed += 1;
        let current_percent = processed * 100 / total_hashes;
        if current_percent >= next_percent {
            eprintln!("Processed {}% of {} hashes", current_percent, db_path);
            next_percent = current_percent + 1;
        }

        let hash = LittleEndian::read_u64(&k);

        let datasets = match Datasets::from_slice(&v) {
            Some(d) => d,
            None => {
                eprintln!("Warning: could not parse dataset list");
                continue;
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

        let (taxonomy_list, lca_lineage, lca_rank) = if let Some(tax_map) = taxonomy_map {
            let taxonomy_list: Vec<String> = dataset_names
                .iter()
                .filter_map(|name| name.split_whitespace().next())
                .map(strip_accession_version)
                .filter_map(|accession| tax_map.get(accession))
                .cloned()
                .collect();

            let (lineage, rank) = compute_lca_strs(&taxonomy_list);
            (
                Some(taxonomy_list),
                Some(lineage),
                rank.map(|r| r.to_string()),
            )
        } else {
            (None, None, None)
        };

        lca_summary.add_rank(taxonomy_list.as_ref(), lca_rank.as_deref());

        let record = ArrowRecord {
            hash,
            dataset_names,
            taxonomy_list,
            lca_lineage,
            lca_rank,
            ksize,
            scaled: *scaled,
            source: db_basename.clone(),
        };

        sender.send(record)?;
    }
    Ok(lca_summary)
}

// main function
pub fn export_revindex_to_parquet(
    db_paths: Vec<Utf8PathBuf>,
    out_path: Utf8PathBuf,
    tax_paths: Vec<Utf8PathBuf>,
    lca_info_path: Option<Utf8PathBuf>,
    rw: bool,
) -> Result<()> {
    // set up ctrl-c signal handler
    let cancel_flag = Arc::new(AtomicBool::new(false));
    setup_ctrlc_handler(cancel_flag.clone())?;

    // load taxonomy if we have it
    let mut full_tax_map = HashMap::new();

    for path in tax_paths {
        let map = load_taxonomy_map(path)?;
        full_tax_map.extend(map);
    }

    let tax_map = if full_tax_map.is_empty() {
        None
    } else {
        Some(full_tax_map)
    };

    // start arrow writer thread
    let (sender, handle) = start_arrow_writer_thread(out_path, 100_000)?;

    // init LCA summary
    let all_summaries = Arc::new(Mutex::new(Vec::new()));

    // parallelize across all input revindex files
    db_paths
        .par_iter()
        .try_for_each::<_, Result<()>>(|db_path| {
            let lca_summary =
                process_revindex(db_path, &sender, tax_map.as_ref(), rw, cancel_flag.clone())?;
            {
                let mut all = all_summaries.lock().unwrap();
                all.push((db_path, lca_summary));
            }
            Ok(())
        })?;

    drop(sender); // Close the channel
    handle.join().unwrap()?; // Wait for writer to finish

    // write LCA summaries to CSV
    let all_summaries_guard = all_summaries.lock().unwrap();
    let summaries: Vec<(String, LCASummary)> = all_summaries_guard
        .iter()
        .map(|(p, s)| (p.file_name().unwrap().to_string(), s.clone()))
        .collect();

    write_lca_info(lca_info_path.as_deref(), &summaries)?;

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
