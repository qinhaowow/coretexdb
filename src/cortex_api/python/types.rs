//! Python type conversions for CortexDB.
//!
//! This module provides type conversion utilities between Rust and Python types for CortexDB, including:
//! - Document conversion
//! - Vector conversion
//! - Error conversion
//! - Schema conversion
//! - Configuration conversion

use crate::cortex_core::types::{Document, Embedding, Float32Vector};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use serde_json::Value;

/// Convert a Python dictionary to a Document
pub fn py_dict_to_document(py_dict: &PyDict) -> PyResult<Document> {
    // Extract document ID
    let id = py_dict.get_item("id")
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'id' field"))?
        .extract::<String>()?;

    // Extract fields
    let fields = py_dict.get_item("fields")
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'fields' field"))?;

    // Convert fields to serde_json::Value
    let fields_value = py_object_to_json_value(fields)?;

    Ok(Document::new(id, fields_value))
}

/// Convert a Document to a Python dictionary
pub fn document_to_py_dict(py: Python, document: &Document) -> PyResult<PyDict> {
    let py_dict = PyDict::new(py);
    
    // Set ID
    py_dict.set_item("id", document.id())?;
    
    // Set fields
    let fields_py = json_value_to_py_object(py, document.fields())?;
    py_dict.set_item("fields", fields_py)?;
    
    Ok(py_dict)
}

/// Convert a Python list of floats to an Embedding
pub fn py_list_to_embedding(py_list: &PyList) -> PyResult<Box<dyn Embedding>> {
    let mut values = Vec::new();
    
    for item in py_list.iter() {
        values.push(item.extract::<f32>()?);
    }
    
    Ok(Box::new(Float32Vector::from(values)))
}

/// Convert an Embedding to a Python list
pub fn embedding_to_py_list(py: Python, embedding: &dyn Embedding) -> PyResult<PyList> {
    let py_list = PyList::new(py, embedding.values());
    Ok(py_list)
}

/// Convert a Python object to a serde_json::Value
pub fn py_object_to_json_value(py_obj: &PyAny) -> PyResult<Value> {
    if py_obj.is_instance_of::<PyDict>() {
        let py_dict = py_obj.downcast::<PyDict>()?;
        let mut map = serde_json::Map::new();
        
        for (key, value) in py_dict.iter() {
            let key_str = key.extract::<String>()?;
            let value_json = py_object_to_json_value(value)?;
            map.insert(key_str, value_json);
        }
        
        Ok(Value::Object(map))
    } else if py_obj.is_instance_of::<PyList>() {
        let py_list = py_obj.downcast::<PyList>()?;
        let mut array = Vec::new();
        
        for item in py_list.iter() {
            array.push(py_object_to_json_value(item)?);
        }
        
        Ok(Value::Array(array))
    } else if py_obj.is_instance_of::<pyo3::types::PyString>() {
        Ok(Value::String(py_obj.extract::<String>()?))
    } else if py_obj.is_instance_of::<pyo3::types::PyFloat>() {
        Ok(Value::Number(serde_json::Number::from_f64(py_obj.extract::<f64>()?).unwrap()))
    } else if py_obj.is_instance_of::<pyo3::types::PyInt>() {
        Ok(Value::Number(serde_json::Number::from(py_obj.extract::<i64>()?)))
    } else if py_obj.is_instance_of::<pyo3::types::PyBool>() {
        Ok(Value::Bool(py_obj.extract::<bool>()?))
    } else if py_obj.is_none() {
        Ok(Value::Null)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!("Unsupported type: {}", py_obj.get_type())
        ))
    }
}

/// Convert a serde_json::Value to a Python object
pub fn json_value_to_py_object(py: Python, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Object(map) => {
            let py_dict = PyDict::new(py);
            for (key, value) in map {
                let value_py = json_value_to_py_object(py, value)?;
                py_dict.set_item(key, value_py)?;
            }
            Ok(py_dict.to_object(py))
        }
        Value::Array(array) => {
            let py_list = PyList::new(py, Vec::<PyObject>::new());
            for value in array {
                let value_py = json_value_to_py_object(py, value)?;
                py_list.append(value_py)?;
            }
            Ok(py_list.to_object(py))
        }
        Value::String(s) => {
            Ok(pyo3::types::PyString::new(py, s).to_object(py))
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(pyo3::types::PyInt::new(py, i).to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(pyo3::types::PyFloat::new(py, f).to_object(py))
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Unsupported number type"))
            }
        }
        Value::Bool(b) => {
            Ok(pyo3::types::PyBool::new(py, *b).to_object(py))
        }
        Value::Null => {
            Ok(py.None())
        }
    }
}

/// Convert a CortexError to a Python exception
pub fn cortex_error_to_py_err(error: &crate::cortex_core::error::CortexError) -> PyErr {
    match error {
        crate::cortex_core::error::CortexError::Api(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Collection(_) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Document(_) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Index(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Query(_) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Storage(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Config(_) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string())
        }
        crate::cortex_core::error::CortexError::Internal(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(error.to_string())
        }
    }
}
