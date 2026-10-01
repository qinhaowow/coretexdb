// CoreTexDB C example — full round trip through include/coretexdb.h.
//
// Build and run with scripts/build_ffi_example.sh:
//   cargo build --lib, then cc against the header, then execute.
// On failure every call prints its status plus coretexdb_last_error().
#include "coretexdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr)                                                        \
    do {                                                                   \
        int rc = (expr);                                                   \
        if (rc != CORETEXDB_OK) {                                          \
            fprintf(stderr, "%s failed with status %d: %s\n", #expr, rc,   \
                    coretexdb_last_error());                               \
            exit(1);                                                       \
        }                                                                  \
    } while (0)

static void print_json(const char* label, char* json) {
    printf("%-24s %s\n", label, json);
    coretexdb_free_string(json);
}

int main(int argc, char** argv) {
    const char* path = argc > 1 ? argv[1] : "/tmp/coretexdb_c_example";

    printf("CoreTexDB %s (header %d.%d.%d)\n", coretexdb_version(),
           CORETEXDB_VERSION_MAJOR, CORETEXDB_VERSION_MINOR,
           CORETEXDB_VERSION_PATCH);

    coretexdb_handle* db = NULL;
    CHECK(coretexdb_open(path, &db));

    CHECK(coretexdb_create_collection(db, "docs", 4, "euclidean"));

    const float v0[] = {0.f, 1.f, 0.f, 0.f};
    const float v1[] = {1.f, 1.f, 0.f, 0.f};
    const float v3[] = {3.f, 1.f, 0.f, 0.f};
    CHECK(coretexdb_insert_vector(db, "docs", "v0", v0, 4,
                                  "{\"text\":\"alpha release\",\"n\":0}"));
    CHECK(coretexdb_insert_vector(db, "docs", "v1", v1, 4,
                                  "{\"text\":\"beta notes\",\"n\":1}"));
    CHECK(coretexdb_insert_vector(db, "docs", "v3", v3, 4,
                                  "{\"text\":\"alpha archive\",\"n\":3}"));

    uint64_t n = 0;
    CHECK(coretexdb_count(db, "docs", &n));
    printf("%-24s %llu\n", "count:", (unsigned long long)n);

    char* json = NULL;
    const float query[] = {0.f, 1.f, 0.f, 0.f};
    CHECK(coretexdb_search(db, "docs", query, 4, 2, NULL, &json));
    print_json("vector search top-2:", json);

    CHECK(coretexdb_search(db, "docs", query, 4, 4, "{\"n\":{\"$gte\":1}}",
                           &json));
    print_json("filtered (n >= 1):", json);

    CHECK(coretexdb_hybrid_search(db, "docs", query, 4, "alpha", 5, NULL,
                                  NULL, &json));
    print_json("hybrid alpha:", json);

    // Error paths are data too: unknown collection must be reported, not
    // crash, and json must stay NULL on failure.
    json = NULL;
    int rc = coretexdb_search(db, "missing", query, 4, 1, NULL, &json);
    printf("%-24s status %d: %s\n", "missing collection:", rc,
           coretexdb_last_error());
    if (rc != CORETEXDB_ERR_NOT_FOUND || json != NULL) {
        fprintf(stderr, "expected NOT_FOUND with NULL json\n");
        exit(1);
    }

    char* names = NULL;
    CHECK(coretexdb_list_collections(db, &names));
    print_json("collections:", names);

    coretexdb_close(db);
    printf("ok\n");
    return 0;
}
