/*
 * CoreTexDB C API
 *
 * Generated-by-hand companion of `src/coretex_ffi.rs`; the Rust test
 * `header_matches_rust_ffi_surface` (tests/ffi_api.rs) fails if the two
 * drift apart, and `scripts/build_ffi_example.sh` compiles and runs
 * `share/examples/c/main.c` against this header.
 *
 * Contract
 * --------
 * - Status: every fallible call returns int. CORETEXDB_OK (0) means
 *   success; a negative CORETEXDB_ERR_* code identifies the failure class.
 *   The message for the most recent failure *on the calling thread* is
 *   returned by coretexdb_last_error(); each call clears it on entry, so
 *   an empty string means the last call on this thread succeeded.
 * - Handles: coretexdb_open allocates; the caller owns the handle and
 *   must pass it to coretexdb_close exactly once. No call may run
 *   concurrently with the close. A handle may otherwise be used from
 *   several threads.
 * - Strings: input C strings are borrowed only for the duration of the
 *   call. JSON returned through an out_json parameter is a heap copy
 *   owned by the caller - release it with coretexdb_free_string(). The
 *   pointer from coretexdb_last_error() must NOT be freed; it is valid
 *   until the next coretexdb_* call on the same thread (or the thread
 *   exits).
 * - NULL: where noted, NULL means "absent" (no filter, no text side);
 *   any other NULL pointer is reported as CORETEXDB_ERR_INVALID_ARG
 *   instead of dereferencing.
 */
#ifndef CORETEXDB_H
#define CORETEXDB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CORETEXDB_VERSION_MAJOR 0
#define CORETEXDB_VERSION_MINOR 2
#define CORETEXDB_VERSION_PATCH 5

/* Status codes (mirrored as Rust consts in coretex_ffi). */
#define CORETEXDB_OK 0
#define CORETEXDB_ERR_INVALID_ARG -1
#define CORETEXDB_ERR_NOT_FOUND -2
#define CORETEXDB_ERR_ALREADY_EXISTS -3
#define CORETEXDB_ERR_DIMENSION -4
#define CORETEXDB_ERR_IO -5
#define CORETEXDB_ERR_JSON -6
#define CORETEXDB_ERR_PANIC -7
#define CORETEXDB_ERR_INTERNAL -100

/* Opaque database handle (struct coretexdb_handle in C terms). */
typedef struct coretexdb_handle coretexdb_handle;

/* Version string of the linked library; static storage, never freed. */
const char* coretexdb_version(void);

/* Message for the last failing call on this thread; empty after success.
 * Thread-local storage: do not free, do not stash across calls. */
const char* coretexdb_last_error(void);

/* Release JSON returned through an out_json parameter. NULL is a no-op. */
void coretexdb_free_string(char* s);

/* Open (or create) a database rooted at `path` (non-empty UTF-8).
 * *out receives the handle, or NULL on failure. */
int coretexdb_open(const char* path, coretexdb_handle** out);

/* Close a handle; NULL is a no-op. The handle is invalid afterwards. */
void coretexdb_close(coretexdb_handle* h);

/* Create a collection of `dimension`-dimensional vectors under `metric`
 * ("euclidean", "cosine", ...). CORETEXDB_ERR_ALREADY_EXISTS on repeat. */
int coretexdb_create_collection(coretexdb_handle* h, const char* name, uint32_t dimension, const char* metric);

/* Delete a collection and its vectors. CORETEXDB_ERR_NOT_FOUND if absent. */
int coretexdb_delete_collection(coretexdb_handle* h, const char* name);

/* *out_json receives a JSON array of collection names, e.g. ["a","b"]. */
int coretexdb_list_collections(coretexdb_handle* h, char** out_json);

/* Insert (or replace by id) one vector. `metadata_json` is a JSON object
 * ({"text": "..."} feeds hybrid search) or NULL for no metadata.
 * Dimension mismatch against the collection -> CORETEXDB_ERR_DIMENSION. */
int coretexdb_insert_vector(coretexdb_handle* h, const char* collection, const char* id, const float* data, uint32_t len, const char* metadata_json);

/* Delete one vector by id. CORETEXDB_ERR_NOT_FOUND when the id is absent. */
int coretexdb_delete_vector(coretexdb_handle* h, const char* collection, const char* id);

/* Number of vectors in the collection through *out. */
int coretexdb_count(coretexdb_handle* h, const char* collection, uint64_t* out);

/* k nearest neighbours of `query[0..query_len]`.
 * `filter_json` narrows candidates, e.g. {"group":"a"} or
 * {"n":{"$gte":400}}, or NULL for no filter.
 * *out_json receives [{"id":..,"distance":..},...] ordered by increasing
 * distance for the collection's metric. Unknown collection ->
 * CORETEXDB_ERR_NOT_FOUND. */
int coretexdb_search(coretexdb_handle* h, const char* collection, const float* query, uint32_t query_len, uint32_t k, const char* filter_json, char** out_json);

/* Hybrid search: fuse ANN neighbours with BM25 text matches over
 * metadata[text_field] by reciprocal rank fusion. Either side may be
 * omitted (NULL vector / NULL-or-blank text) but not both.
 * `text_field` NULL selects the default field "text".
 * *out_json receives [{"id":..,"score":..,"sources":["vector"|"text",..]}]
 * ordered by descending fused score. The text index is rebuilt after
 * every write, never served stale. */
int coretexdb_hybrid_search(coretexdb_handle* h, const char* collection, const float* vector, uint32_t vector_len, const char* text, uint32_t k, const char* filter_json, const char* text_field, char** out_json);

#ifdef __cplusplus
}
#endif

#endif /* CORETEXDB_H */
