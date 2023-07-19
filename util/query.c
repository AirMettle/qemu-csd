/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include "qemu/query.h"
#include <stdio.h>
#include <unistd.h>
#include "duckdb/duckdb.h"
#include "kv_utils.h"
#include "qemu/osdep.h"
#include "qemu/job.h"
#include <stdatomic.h>

duckdb_database db;
duckdb_connection* cons;
bool* busy;
int num_connections;
QemuMutex connection_mutex;

int query_init_db(int num_connection) {
    num_connections = num_connection;
    if (duckdb_open(NULL, &db) == DuckDBError) {
        fprintf(stderr, "Cannot open DuckDB!\n");
        return KV_ERROR_DUCKDB;
    }
    cons = malloc(num_connection * sizeof(duckdb_connection));
    if (!cons) {
        fprintf(stderr, "Cannot allocate memory!\n");
        return KV_ERROR_MEMORY_ALLOCATION;
    }
    for (int i = 0; i < num_connection; ++i) {
        if (duckdb_connect(db, &cons[i]) == DuckDBError) {
            fprintf(stderr, "Cannot connect DuckDB!\n");
            for (int j = 0; j < i; ++i) {
                duckdb_disconnect(&cons[j]);
            }
            free(cons);
            duckdb_close(&db);
            return KV_ERROR_DUCKDB;
        }
    }
    busy = malloc(num_connection * sizeof(bool));
    if (!busy) {
        fprintf(stderr, "Cannot allocate memory!\n");
        for (int i = 0; i < num_connection; ++i) {
            duckdb_disconnect(&cons[i]);
        }
        free(cons);
        duckdb_close(&db);
        return KV_ERROR_MEMORY_ALLOCATION;
    }
    for (int i = 0; i < num_connection; ++i) {
        busy[i] = false;
    }
    qemu_mutex_init(&connection_mutex);
    return 0;
}

void query_close_db(void) {
    for (int i = 0; i < num_connections; ++i) {
        duckdb_disconnect(&cons[i]);
    }
    free(cons);
    free(busy);
    qemu_mutex_destroy(&connection_mutex);
    duckdb_close(&db);
}

int
run_query(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_length,
          char *sql, size_t *output_len, Query_Data_Type input_format,
          Query_Data_Type output_format, bool use_csv_headers_input,
          bool use_csv_headers_output, unsigned char **result) {
    // construct the command string
    const char *path = get_path_str(bus_number, namespace_id, key, key_length, false);
    if (!path) {
        return KV_ERROR_FILE_PATH;
    }
    size_t path_len = strlen(path);
    char *end = strcasestr(sql, "from");
    if (!end) {
         return KV_ERROR_INVALID_PARAMETER;
    }
    size_t sql_first_part_len = end - sql + 5;
    size_t total_sql_len = strlen(sql);

    // remove the ';' at the end
    if (sql[total_sql_len - 1] == ';') {
        --total_sql_len;
    }

    // find the position of the start of sql clause
    size_t sql_second_part_pos = sql_first_part_len;
    while (sql_second_part_pos < total_sql_len && sql[sql_second_part_pos] != ' ') {
        ++sql_second_part_pos;
    }

    char command[6 + sql_first_part_len + 16 + path_len + 84 + total_sql_len - sql_second_part_pos];
    size_t pos = 0;
    strcpy(command, "copy (");
    pos += 6;
    strncpy(command + pos, sql, sql_first_part_len);
    pos += sql_first_part_len;
    switch (input_format) {
        case QUERY_TYPE_JSON:
            strcpy(command + pos, "read_json_auto('");
            pos += 16;
            break;
        case QUERY_TYPE_CSV:
            strcpy(command + pos, "read_csv_auto('");
            pos += 15;
            break;
        case QUERY_TYPE_PARQUET:
            strcpy(command + pos, "read_parquet('");
            pos += 14;
            break;
    }

    strcpy(command + pos, path);
    pos += path_len;
    free((void*)path);

    command[pos++] = '\'';
    if (input_format == QUERY_TYPE_CSV) {
        if (use_csv_headers_input) {
            strcpy(command + pos, ", HEADER=TRUE");
            pos += 13;
        } else {
            strcpy(command + pos, ", HEADER=FALSE");
            pos += 14;
        }
    }
    command[pos++] = ')';
    strcpy(command + pos, sql + sql_second_part_pos);
    pos += total_sql_len - sql_second_part_pos;

    strcpy(command + pos, ") to '");
    pos += 6;

    char result_path[32 + 8 + 1];
    size_t result_path_pos = 0;
    // use a counter to avoid file name conflicts of result files
    static atomic_uint counter = ATOMIC_VAR_INIT(0);
    result_path_pos += sprintf(result_path + result_path_pos, "%u", atomic_fetch_add(&counter, 1));
    switch (output_format) {
        case QUERY_TYPE_JSON:
            strcpy(result_path + result_path_pos, ".json");
            result_path_pos += 5;
            break;
        case QUERY_TYPE_CSV:
            strcpy(result_path + result_path_pos, ".csv");
            result_path_pos += 4;
            break;
        case QUERY_TYPE_PARQUET:
            strcpy(result_path + result_path_pos, ".parquet");
            result_path_pos += 8;
            break;
    }
    strcpy(command + pos, result_path);
    pos += result_path_pos;
    command[pos++] = '\'';

    if (output_format == QUERY_TYPE_CSV && use_csv_headers_output) {
        strcpy(command + pos, " ( header )");
        pos += 11;
    } else if (output_format == QUERY_TYPE_PARQUET) {
        strcpy(command + pos, " ( format parquet )");
        pos += 19;
    }
    command[pos] = '\0';

    int con_id = -1;
    do {
        qemu_mutex_lock(&connection_mutex);
        for (int i = 0; i < num_connections; ++i) {
            if (!busy[i]) {
                busy[i] = true;
                con_id = i;
                break;
            }
        }
        qemu_mutex_unlock(&connection_mutex);
        if (con_id == -1) {
            usleep(100000);
        }
    } while (con_id == -1);
    duckdb_state state = duckdb_query(cons[con_id], command, NULL);
    qemu_mutex_lock(&connection_mutex);
    busy[con_id] = false;
    qemu_mutex_unlock(&connection_mutex);
    if (state == DuckDBError) {
        return KV_ERROR_QUERY;
    }

    FILE *file;
    file = fopen(result_path, "r");
    if (file == NULL) {
        return KV_ERROR_CANNOT_OPEN;
    }
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    rewind(file);
    unsigned char *buffer = malloc(file_size);
    if (buffer == NULL) {
        fclose(file);
        remove(result_path);
        return KV_ERROR_MEMORY_ALLOCATION;
    }
    size_t read_bytes = fread(buffer, sizeof(char), file_size, file);
    if (read_bytes != file_size) {
        free(buffer);
        fclose(file);
        remove(result_path);
        return KV_ERROR_FILE_READ;
    }
    fclose(file);
    *output_len = read_bytes;
    *result = buffer;
    remove(result_path);
    return 0;
}
