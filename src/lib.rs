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
#[pyo3(signature = (db, output, rw = false))]
fn do_export_to_parquet(db: String, output: String, rw: bool) -> anyhow::Result<u8> {
    let db_path = Utf8PathBuf::from(db);
    let output_path = Utf8PathBuf::from(output);

    if !is_revindex_database(&db_path) {
        bail!("'{}' is not a valid RevIndex database", &db_path);
    }

    match export_revindex_to_parquet(&db_path, &output_path, rw) {
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
