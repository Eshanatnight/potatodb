/**
 * Using the PotatoDB plain C API directly.
 *
 * This example uses only the C header (potatodb.h) — no C++ wrapper,
 * no RAII, no std::string. It demonstrates the exact same workflow
 * that a C program (or any FFI consumer) would follow.
 *
 * Build (as C++, but only C API is used):
 *   cargo build --release -p potatodb-ffi
 *   gcc -std=c11 -I../include c_api.c \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -lstdc++ -o c_api
 *
 * Or compile as C++:
 *   g++ -std=c++17 -I../include c_api.cpp \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -o c_api
 */

#include "potatodb.h"

#include <stdio.h>
#include <stdlib.h>

/// Print result in the most common patterns depending on kind.
static void print_result(const char *label, potato_result *res) {
    printf("── %s ──\n", label);
    potato_result_kind kind = potato_result_get_kind(res);
    if (kind == POTATO_RESULT_MESSAGE) {
        const char *msg = potato_result_message(res);
        if (msg) printf("  %s\n\n", msg);
    } else {
        const char *disp = potato_result_display(res);
        if (disp) printf("%s\n\n", disp);
    }
}

int main(void) {
    /* ── Open ─────────────────────────────────────────────── */
    potato_db *db = potato_open_local("./ffi_c_api_data");
    if (!db) {
        fprintf(stderr, "Failed to open database\n");
        return EXIT_FAILURE;
    }

    /* ── Create table ─────────────────────────────────────── */
    potato_result *res = potato_execute(db,
        "CREATE TABLE IF NOT EXISTS sensors "
        "(id INT, location VARCHAR, reading DOUBLE, active BOOLEAN);");
    if (!res) {
        fprintf(stderr, "create: %s\n", potato_last_error(db));
        potato_close(db);
        return EXIT_FAILURE;
    }
    print_result("CREATE TABLE", res);
    potato_result_free(res);

    /* ── Insert rows ──────────────────────────────────────── */
    const char *inserts[] = {
        "INSERT INTO sensors VALUES (1, 'Lab-A',  23.5,  true);",
        "INSERT INTO sensors VALUES (2, 'Lab-B',  19.8,  true);",
        "INSERT INTO sensors VALUES (3, 'Lab-C',  25.1,  false);",
        "INSERT INTO sensors VALUES (4, 'Lab-A',  22.0,  true);",
        "INSERT INTO sensors VALUES (5, 'Lab-B',  20.3,  false);",
    };
    for (size_t i = 0; i < sizeof(inserts) / sizeof(inserts[0]); ++i) {
        res = potato_execute(db, inserts[i]);
        if (!res) {
            fprintf(stderr, "insert: %s\n", potato_last_error(db));
            potato_close(db);
            return EXIT_FAILURE;
        }
        potato_result_free(res);
    }
    printf("Inserted %zu rows.\n\n",
           sizeof(inserts) / sizeof(inserts[0]));

    /* ── Select all ───────────────────────────────────────── */
    res = potato_execute(db, "SELECT * FROM sensors ORDER BY id;");
    if (!res) {
        fprintf(stderr, "select: %s\n", potato_last_error(db));
        potato_close(db);
        return EXIT_FAILURE;
    }
    print_result("SELECT *", res);

    /* ── Column metadata ──────────────────────────────────── */
    printf("── Column metadata ──\n");
    size_t ncols = potato_result_column_count(res);
    for (size_t c = 0; c < ncols; ++c) {
        const char *name = potato_result_column_name(res, c);
        potato_column_type ty = potato_result_get_column_type(res, c);
        printf("  [%zu] %-12s type=%d\n", c, name ? name : "(null)", ty);
    }
    printf("\n");

    /* ── Row-level value access ───────────────────────────── */
    printf("── Row-level access ──\n");
    size_t nrows = potato_result_row_count(res);
    for (size_t r = 0; r < nrows; ++r) {
        long long id = potato_result_get_int(res, r, 0);

        char *location = potato_result_get_string(res, r, 1);
        double reading = potato_result_get_double(res, r, 2);
        bool active    = potato_result_get_bool(res, r, 3);

        printf("  id=%lld  location=%-6s  reading=%.1f  active=%s\n",
               id,
               location ? location : "(null)",
               reading,
               active ? "true" : "false");

        potato_string_free(location);
    }
    printf("\n");
    potato_result_free(res);

    /* ── NULL handling ────────────────────────────────────── */
    potato_execute(db,
        "INSERT INTO sensors VALUES (6, NULL, NULL, NULL);");

    res = potato_execute(db,
        "SELECT * FROM sensors WHERE id = 6;");
    if (res) {
        printf("── NULL handling ──\n");
        for (size_t c = 0; c < potato_result_column_count(res); ++c) {
            bool is_null = potato_result_is_null(res, 0, c);
            printf("  col %zu (%s): %s\n",
                   c,
                   potato_result_column_name(res, c),
                   is_null ? "NULL" : "has value");
        }
        printf("\n");
        potato_result_free(res);
    }

    /* ── Flush + storage stats ────────────────────────────── */
    if (potato_flush_table(db, "sensors") == 0) {
        printf("── Storage stats ──\n");
        long long files = potato_table_parquet_file_count(db, "sensors");
        long long bytes = potato_table_total_bytes(db, "sensors");
        printf("  parquet files: %lld\n", files);
        printf("  total bytes:   %lld\n\n", bytes);
    }

    /* ── Cleanup ──────────────────────────────────────────── */
    res = potato_execute(db, "DROP TABLE sensors;");
    if (res) {
        print_result("DROP TABLE", res);
        potato_result_free(res);
    }

    potato_close(db);
    printf("All C API operations completed successfully.\n");
    return EXIT_SUCCESS;
}
