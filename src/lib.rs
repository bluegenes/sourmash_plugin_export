use camino::Utf8PathBuf;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use anyhow::bail;

mod export;
use export::export_revindex_to_parquet;

#[pyfunction]
fn set_global_thread_pool(num_threads: usize) -> PyResult<usize> {
    if std::panic::catch_unwind(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
    })
    .is_ok()
    {
        Ok(rayon::current_num_threads())
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Could not set the number of threads. Global thread pool might already be initialized.",
        ))
    }
}

pub fn is_revindex_database(path: &Utf8PathBuf) -> bool {
    // quick file check for Revindex database:
    // is path a directory that contains a file named 'CURRENT'?
    if path.is_dir() {
        let current_file = path.join("CURRENT");
        current_file.exists() && current_file.is_file()
    } else {
        false
    }
}

#[pyfunction]
#[pyo3(signature = (db_path_list, output, tax_path_list = None, lca_info_path= None, rw = false))]
fn do_export_to_parquet(
    db_path_list: Vec<String>,
    output: String,
    tax_path_list: Option<Vec<String>>,
    lca_info_path: Option<String>,
    rw: bool,
) -> anyhow::Result<u8> {
    let db_paths: Vec<Utf8PathBuf> = db_path_list.into_iter().map(Utf8PathBuf::from).collect();
    let tax_paths: Vec<Utf8PathBuf> = tax_path_list
        .unwrap_or_default()
        .into_iter()
        .map(Utf8PathBuf::from)
        .collect();
    let output_path = Utf8PathBuf::from(output);
    let lca_info_path = lca_info_path.map(Utf8PathBuf::from);

    for db in &db_paths {
        if !is_revindex_database(db) {
            bail!("'{db}' is not a valid RevIndex database");
        }
    }

    match export_revindex_to_parquet(db_paths, output_path, tax_paths, lca_info_path, rw) {
        Ok(_) => Ok(0),
        Err(e) => {
            eprintln!("Error: {e}");
            Ok(1)
        }
    }
}

#[pymodule]
fn sourmash_plugin_export(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(do_export_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(set_global_thread_pool, m)?)?;
    Ok(())
}
