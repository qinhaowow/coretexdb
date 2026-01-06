//! Python utilities for CortexDB.
//!
//! This module provides utility functions for the CortexDB Python bindings, including:
//! - Type conversion utilities
//! - Error handling utilities
//! - Logging utilities
//! - Configuration utilities
//! - Miscellaneous helper functions

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::sync::Arc;

/// Convert a Python dictionary to a Rust hash map
pub fn py_dict_to_hash_map(py_dict: &PyDict) -> PyResult<std::collections::HashMap<String, PyObject>> {
    let mut hash_map = std::collections::HashMap::new();
    
    for (key, value) in py_dict.iter() {
        let key_str = key.extract::<String>()?;
        hash_map.insert(key_str, value.to_object(py_dict.py()));
    }
    
    Ok(hash_map)
}

/// Convert a Rust hash map to a Python dictionary
pub fn hash_map_to_py_dict(py: Python, hash_map: &std::collections::HashMap<String, PyObject>) -> PyResult<PyDict> {
    let py_dict = PyDict::new(py);
    
    for (key, value) in hash_map {
        py_dict.set_item(key, value)?;
    }
    
    Ok(py_dict)
}

/// Extract a string from a Python object, handling both str and bytes
pub fn extract_string(py_obj: &PyAny) -> PyResult<String> {
    if py_obj.is_instance_of::<PyString>() {
        py_obj.extract::<String>()
    } else if py_obj.is_instance_of::<pyo3::types::PyBytes>() {
        let bytes = py_obj.downcast::<pyo3::types::PyBytes>()?;
        String::from_utf8(bytes.as_bytes().to_vec())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected str or bytes"
        ))
    }
}

/// Get the current Python version
pub fn get_python_version(py: Python) -> String {
    let sys = py.import("sys").unwrap();
    let version = sys.getattr("version").unwrap().extract::<String>().unwrap();
    version
}

/// Check if a Python package is installed
pub fn is_package_installed(py: Python, package_name: &str) -> bool {
    let importlib = py.import("importlib.util").unwrap();
    let spec = importlib.call_method1("find_spec", (package_name,)).unwrap();
    !spec.is_none()
}

/// Set up Python logging for CortexDB
pub fn setup_python_logging(py: Python, level: &str) -> PyResult<()> {
    let logging = py.import("logging")?;
    logging.call_method1("basicConfig", ())?;
    let logger = logging.call_method1("getLogger", ("cortexdb",))?;
    logger.call_method1("setLevel", (level,))?;
    Ok(())
}

/// Convert a Rust result to a Python result, mapping errors appropriately
pub fn result_to_py_result<T, E>(result: Result<T, E>) -> PyResult<T>
where
    E: std::fmt::Display,
{
    result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

/// Get the current working directory from Python
pub fn get_current_working_directory(py: Python) -> PyResult<String> {
    let os = py.import("os")?;
    let cwd = os.call_method0("getcwd")?.extract::<String>()?;
    Ok(cwd)
}

/// Join paths using Python's os.path.join
pub fn join_paths(py: Python, paths: &[&str]) -> PyResult<String> {
    let os = py.import("os")?;
    let path = os.getattr("path")?;
    let joined = path.call_method1("join", (PyList::new(py, paths),))?.extract::<String>()?;
    Ok(joined)
}
