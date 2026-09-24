//! CoreTexDB C API (placeholder for V0.2.2 packaging).
//! Full FFI surface lands with the C bindings module.
#ifndef CORETEXDB_H
#define CORETEXDB_H

#ifdef __cplusplus
extern "C" {
#endif

#define CORETEXDB_VERSION_MAJOR 0
#define CORETEXDB_VERSION_MINOR 2
#define CORETEXDB_VERSION_PATCH 2

const char* coretexdb_version(void);

#ifdef __cplusplus
}
#endif

#endif /* CORETEXDB_H */
