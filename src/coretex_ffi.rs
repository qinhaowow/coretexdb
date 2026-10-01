//! C ABI surface behind `include/coretexdb.h`.
//!
//! Contract for every `coretexdb_*` entry point:
//!
//! * **Handles** — [`coretexdb_open`] boxes a [`CoreTexDbHandle`] (the
//!   async database plus a dedicated tokio runtime) and hands the raw
//!   pointer to C. The caller owns it and must pass it to
//!   [`coretexdb_close`] exactly once; no other call may run concurrently
//!   with that close.
//! * **Strings** — inputs are borrowed only for the duration of the call.
//!   Outputs (`out_json` parameters) are heap allocations owned by the
//!   caller and released with [`coretexdb_free_string`]. The pointer from
//!   [`coretexdb_last_error`] is the exception: it is thread-local, must
//!   *not* be freed, and stays valid until the next `coretexdb_*` call on
//!   the same thread (or until the thread exits).
//! * **Status** — `0` ([`CORETEXDB_OK`]) on success, a negative
//!   `CORETEXDB_ERR_*` code otherwise; the human-readable reason for the
//!   most recent failure is in [`coretexdb_last_error`]. Every call clears
//!   that message on entry, so an empty string means the last call on this
//!   thread succeeded.
//! * **Panics** — a Rust panic must never unwind across the ABI (that is
//!   undefined behaviour in C); every entry point catches panics and
//!   reports [`CORETEXDB_ERR_PANIC`].
//! * **Concurrency** — a handle may be used from several threads (the
//!   database core is internally synchronised, and the wrappers only take
//!   shared borrows of it); only `close` must not race with other calls.
//!
//! Every dereference of a caller-owned pointer sits inside an explicit
//! `unsafe { }` block, so the module also compiles under the stricter
//! `unsafe_op_in_unsafe_fn` rules of later Rust editions.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::{CoreTexDB, CoreTexError, DbConfig, HybridSearchRequest};

/// Call succeeded.
pub const CORETEXDB_OK: c_int = 0;
/// A required pointer/argument was null, empty, or not valid UTF-8/JSON.
pub const CORETEXDB_ERR_INVALID_ARG: c_int = -1;
/// The collection or vector does not exist.
pub const CORETEXDB_ERR_NOT_FOUND: c_int = -2;
/// The collection already exists.
pub const CORETEXDB_ERR_ALREADY_EXISTS: c_int = -3;
/// Vector length does not match the collection dimension.
pub const CORETEXDB_ERR_DIMENSION: c_int = -4;
/// Underlying storage I/O failed.
pub const CORETEXDB_ERR_IO: c_int = -5;
/// Supplied JSON failed to parse (or a result failed to serialise).
pub const CORETEXDB_ERR_JSON: c_int = -6;
/// A Rust panic was caught at the boundary instead of unwinding into C.
pub const CORETEXDB_ERR_PANIC: c_int = -7;
/// Unclassified internal error (see `coretexdb_last_error`).
pub const CORETEXDB_ERR_INTERNAL: c_int = -100;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").expect("static empty string"));
}

/// Record the message `coretexdb_last_error` will report on this thread.
///
/// Never panics: TLS may already be torn down during thread exit, and a
/// NUL inside the message would only garble the C string — both cases
/// degrade instead of unwinding across the ABI.
fn set_last_error(msg: &str) {
    let mut clean = String::with_capacity(msg.len());
    clean.extend(msg.chars().map(|c| if c == '\0' { '\u{fffd}' } else { c }));
    let owned = CString::new(clean).unwrap_or_else(|_| CString::new("error").expect("static"));
    let _ = LAST_ERROR.try_with(|slot| *slot.borrow_mut() = owned);
}

/// Message for the most recent failing `coretexdb_*` call **on this
/// thread**; empty after a successful call.
///
/// The pointer belongs to thread-local storage: it stays valid until the
/// next `coretexdb_*` call on this same thread overwrites it, or the
/// thread exits. Read it immediately after a failure; never free it.
#[no_mangle]
pub extern "C" fn coretexdb_last_error() -> *const c_char {
    LAST_ERROR
        .try_with(|slot| slot.borrow().as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Release a JSON string returned through an `out_json` parameter.
/// NULL is a no-op. The pointer is invalid afterwards.
///
/// # Safety
/// `s` must come from one of this library's `out_json` parameters (or be
/// NULL) and must not be freed twice.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // from_raw/to_raw are inverse operations; the wrapper cannot panic.
    drop(unsafe { CString::from_raw(s) });
}

/// Map a library error onto the stable C status codes.
fn status_of(e: &CoreTexError) -> c_int {
    use CoreTexError as E;
    match e {
        E::CollectionNotFound(_)
        | E::DocumentNotFound(_)
        | E::IndexNotFound(_)
        | E::NodeNotFound(_)
        | E::EdgeNotFound(_)
        | E::TransactionNotFound(_)
        | E::SnapshotNotFound(_)
        | E::CheckpointNotFound(_)
        | E::BackupNotFound(_) => CORETEXDB_ERR_NOT_FOUND,
        E::CollectionAlreadyExists(_) | E::NodeAlreadyExists(_) | E::EdgeAlreadyExists(_) => {
            CORETEXDB_ERR_ALREADY_EXISTS
        }
        E::DimensionMismatch { .. } | E::InvalidDimension(_) => CORETEXDB_ERR_DIMENSION,
        E::ValidationError(_) => CORETEXDB_ERR_INVALID_ARG,
        E::Io(_) => CORETEXDB_ERR_IO,
        E::Serialization(_) => CORETEXDB_ERR_JSON,
        _ => CORETEXDB_ERR_INTERNAL,
    }
}

/// Run one entry-point body with the full contract:
/// clear the thread-local message, contain panics, translate errors into
/// status codes (recording their text for `coretexdb_last_error`).
fn ffi<T>(body: impl FnOnce() -> Result<T, CoreTexError>) -> Result<T, c_int> {
    set_last_error("");
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
            set_last_error(&e.to_string());
            Err(status_of(&e))
        }
        Err(_) => {
            set_last_error("panic inside CoreTexDB FFI");
            Err(CORETEXDB_ERR_PANIC)
        }
    }
}

fn invalid(msg: &str) -> CoreTexError {
    CoreTexError::ValidationError(msg.to_string())
}

/// Borrow a caller C string for the duration of one call.
fn cstr<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, CoreTexError> {
    if ptr.is_null() {
        return Err(invalid(&format!("{what} is null")));
    }
    let s = unsafe { CStr::from_ptr(ptr) };
    s.to_str()
        .map_err(|_| invalid(&format!("{what} is not valid UTF-8")))
}

/// Like [`cstr`], but NULL means "absent".
fn optional_cstr<'a>(ptr: *const c_char, what: &str) -> Result<Option<&'a str>, CoreTexError> {
    if ptr.is_null() {
        Ok(None)
    } else {
        cstr(ptr, what).map(Some)
    }
}

/// Move a serialised result into caller-owned memory through `out`.
fn write_out(json: &str, out: *mut *mut c_char) -> Result<(), CoreTexError> {
    if out.is_null() {
        return Err(invalid("out is null"));
    }
    let owned = CString::new(json)
        .map_err(|_| CoreTexError::Other("serialised result contains an interior NUL".into()))?;
    unsafe { *out = owned.into_raw() };
    Ok(())
}

/// One open database: the async core plus the runtime that drives it.
///
/// Exposed as an opaque type (`struct coretexdb_handle` in C). Wrappers only
/// ever take *shared* borrows (`Runtime::block_on` and every `CoreTexDB`
/// method take `&self`), so concurrent calls on one handle are sound; the
/// caller still guarantees that `coretexdb_close` does not overlap them.
pub struct CoreTexDbHandle {
    db: CoreTexDB,
    rt: tokio::runtime::Runtime,
}

/// Open (or create) a database rooted at `path` and return a handle
/// through `*out` (NULL on failure). `path` must be a non-empty, valid
/// UTF-8 C string; the directory layout is created when missing and
/// existing data is loaded.
///
/// # Safety
/// `out` must be a valid, writable `coretexdb_handle**` for the duration
/// of the call, and `path` a readable NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_open(
    path: *const c_char,
    out: *mut *mut CoreTexDbHandle,
) -> c_int {
    ffi(|| {
        if out.is_null() {
            return Err(invalid("open: out is null"));
        }
        unsafe { *out = std::ptr::null_mut() };
        let path = cstr(path, "path")?;
        if path.trim().is_empty() {
            return Err(invalid("open: path must not be empty"));
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| CoreTexError::Other(format!("tokio runtime: {e}")))?;
        let db = CoreTexDB::with_config(DbConfig::new(path));
        rt.block_on(db.init())?;
        unsafe { *out = Box::into_raw(Box::new(CoreTexDbHandle { db, rt })) };
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Close a handle from [`coretexdb_open`]; NULL is a no-op. The handle is
/// invalid afterwards — using it again is undefined behaviour, exactly as
/// with `free` in C.
///
/// # Safety
/// `h` must be a live handle that has not been closed yet, and no thread
/// may be inside another call on it while it closes.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_close(h: *mut CoreTexDbHandle) {
    if h.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(h));
    }));
}

/// Create collection `name` holding `dimension`-dimensional vectors under
/// `metric` (`"euclidean"`, `"cosine"`, ...). Fails with
/// [`CORETEXDB_ERR_ALREADY_EXISTS`] when it is already there.
///
/// # Safety
/// `h` must be a live handle; `name` and `metric` readable C strings.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_create_collection(
    h: *mut CoreTexDbHandle,
    name: *const c_char,
    dimension: u32,
    metric: *const c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let name = cstr(name, "name")?;
        let metric = cstr(metric, "metric")?;
        if dimension == 0 {
            return Err(invalid("dimension must be greater than 0"));
        }
        h.rt
            .block_on(h.db.create_collection(name, dimension as usize, metric))?;
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Delete collection `name` and everything in it
/// ([`CORETEXDB_ERR_NOT_FOUND`] when absent).
///
/// # Safety
/// `h` must be a live handle; `name` a readable C string.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_delete_collection(
    h: *mut CoreTexDbHandle,
    name: *const c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let name = cstr(name, "name")?;
        h.rt.block_on(h.db.delete_collection(name))?;
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// List collection names as a JSON array (`["a","b"]`) through `*out_json`;
/// release it with [`coretexdb_free_string`].
///
/// # Safety
/// `h` must be a live handle; `out_json` a valid writable `char**`.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_list_collections(
    h: *mut CoreTexDbHandle,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        if out_json.is_null() {
            return Err(invalid("out_json is null"));
        }
        unsafe { *out_json = std::ptr::null_mut() };
        let names = h.rt.block_on(h.db.list_collections())?;
        write_out(&serde_json::to_string(&names)?, out_json)
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Insert (or overwrite) one vector: `data[0..len]` are the components,
/// `metadata_json` a JSON object (`{"text": "..."}` feeds hybrid search)
/// or NULL for none. `id` must be unique within the collection or it
/// replaces the previous record.
///
/// # Safety
/// `h` must be a live handle; `collection`/`id` readable C strings;
/// `data` readable for `len` floats; `metadata_json` NULL or a readable
/// C string.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_insert_vector(
    h: *mut CoreTexDbHandle,
    collection: *const c_char,
    id: *const c_char,
    data: *const f32,
    len: u32,
    metadata_json: *const c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let collection = cstr(collection, "collection")?;
        let id = cstr(id, "id")?;
        if data.is_null() {
            return Err(invalid("data is null"));
        }
        let vector = unsafe { std::slice::from_raw_parts(data, len as usize) }.to_vec();
        let metadata = match optional_cstr(metadata_json, "metadata_json")? {
            Some(s) => serde_json::from_str(s)?,
            None => serde_json::Value::Null,
        };
        h.rt
            .block_on(h.db.insert_vectors(collection, vec![(id.to_string(), vector, metadata)]))?;
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Delete one vector by id ([`CORETEXDB_ERR_NOT_FOUND`] when the id is
/// unknown) and return the number removed through the status code only.
///
/// # Safety
/// `h` must be a live handle; `collection` and `id` readable C strings.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_delete_vector(
    h: *mut CoreTexDbHandle,
    collection: *const c_char,
    id: *const c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let collection = cstr(collection, "collection")?;
        let id = cstr(id, "id")?;
        let removed = h
            .rt
            .block_on(h.db.delete_vectors(collection, &[id.to_string()]))?;
        if removed == 0 {
            return Err(CoreTexError::DocumentNotFound(id.to_string()));
        }
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Number of vectors currently in `collection`, through `*out`.
///
/// # Safety
/// `h` must be a live handle; `collection` a readable C string; `out` a
/// valid writable `uint64_t*`.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_count(
    h: *mut CoreTexDbHandle,
    collection: *const c_char,
    out: *mut u64,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let collection = cstr(collection, "collection")?;
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        let n = h.rt.block_on(h.db.get_vectors_count(collection))?;
        unsafe { *out = n as u64 };
        Ok(())
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Nearest-neighbour search: `k` closest vectors to `query[0..query_len]`.
/// `filter_json` (or NULL) narrows the candidates, e.g.
/// `{"group":"a"}` or `{"n":{"$gte":400}}`.
///
/// `*out_json` receives an array of `{"id":..,"distance":..}` ordered by
/// increasing distance for the collection's metric; release it with
/// [`coretexdb_free_string`]. An unknown collection is
/// [`CORETEXDB_ERR_NOT_FOUND`].
///
/// # Safety
/// `h` must be a live handle; `collection` a readable C string; `query`
/// readable for `query_len` floats; `filter_json` NULL or a readable C
/// string; `out_json` a valid writable `char**`.
#[no_mangle]
pub unsafe extern "C" fn coretexdb_search(
    h: *mut CoreTexDbHandle,
    collection: *const c_char,
    query: *const f32,
    query_len: u32,
    k: u32,
    filter_json: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let collection = cstr(collection, "collection")?;
        if out_json.is_null() {
            return Err(invalid("out_json is null"));
        }
        unsafe { *out_json = std::ptr::null_mut() };
        if query.is_null() {
            return Err(invalid("query is null"));
        }
        let query = unsafe { std::slice::from_raw_parts(query, query_len as usize) }.to_vec();
        let filter = optional_cstr(filter_json, "filter_json")?
            .map(serde_json::from_str)
            .transpose()?;
        let hits = h.rt.block_on(h.db.search(collection, query, k as usize, filter))?;
        let json = serde_json::to_string(
            &hits
                .iter()
                .map(|hit| {
                    serde_json::json!({ "id": hit.id, "distance": hit.distance })
                })
                .collect::<Vec<_>>(),
        )?;
        write_out(&json, out_json)
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Hybrid search: fuse the vector side (ANN) with the BM25 text side over
/// `metadata[text_field]` using reciprocal rank fusion. Either side may be
/// omitted, but not both:
///
/// * `vector` NULL → text-only; `text` NULL/blank → vector-only;
/// * both absent → [`CORETEXDB_ERR_INVALID_ARG`];
/// * `filter_json`/`text_field` NULL → unset (default field is `"text"`).
///
/// `*out_json` receives an array of
/// `{"id":..,"score":..,"sources":["vector"|"text",..]}` sorted by
/// descending fused score; release it with [`coretexdb_free_string`].
/// The text index is rebuilt after every write, never served stale.
///
/// # Safety
/// `h` must be a live handle; `collection` a readable C string; `vector`
/// NULL or readable for `vector_len` floats; `text`, `filter_json`,
/// `text_field` NULL or readable C strings; `out_json` a valid writable
/// `char**`.
#[no_mangle]
#[allow(clippy::too_many_arguments)] // an honest 9-parameter C entry point
pub unsafe extern "C" fn coretexdb_hybrid_search(
    h: *mut CoreTexDbHandle,
    collection: *const c_char,
    vector: *const f32,
    vector_len: u32,
    text: *const c_char,
    k: u32,
    filter_json: *const c_char,
    text_field: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi(|| {
        let h = unsafe { handle(h)? };
        let collection = cstr(collection, "collection")?;
        if out_json.is_null() {
            return Err(invalid("out_json is null"));
        }
        unsafe { *out_json = std::ptr::null_mut() };

        let vector = match vector.is_null() {
            false => Some(unsafe { std::slice::from_raw_parts(vector, vector_len as usize) }.to_vec()),
            true => None,
        };
        let text = optional_cstr(text, "text")?;
        if vector.is_none() && text.map_or(true, |t| t.trim().is_empty()) {
            return Err(invalid(
                "hybrid_search: a vector or a non-empty text is required",
            ));
        }

        let mut request = HybridSearchRequest::new(k as usize);
        if let Some(vector) = vector {
            request = request.with_vector(vector);
        }
        if let Some(text) = text {
            request = request.with_text(text);
        }
        if let Some(filter) = optional_cstr(filter_json, "filter_json")? {
            request = request.with_filter(serde_json::from_str(filter)?);
        }
        if let Some(field) = optional_cstr(text_field, "text_field")? {
            request = request.with_text_field(field);
        }

        let hits = h.rt.block_on(h.db.hybrid_search(collection, request))?;
        write_out(&serde_json::to_string(&hits)?, out_json)
    })
    .err()
    .unwrap_or(CORETEXDB_OK)
}

/// Validate a caller handle before touching it.
///
/// # Safety
/// The raw pointer must either be null or a live [`CoreTexDbHandle`]
/// that outlives the returned reference (checked by the caller's contract
/// in `include/coretexdb.h`).
unsafe fn handle<'a>(raw: *mut CoreTexDbHandle) -> Result<&'a CoreTexDbHandle, CoreTexError> {
    if raw.is_null() {
        return Err(invalid("null database handle (closed or never opened?)"));
    }
    Ok(unsafe { &*raw })
}
