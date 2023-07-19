/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#ifndef QUERY_H
#define QUERY_H

#include <string.h>
#include <stdbool.h>
#include <stdint.h>

typedef enum Query_Data_Type {
    QUERY_TYPE_CSV = 0,
    QUERY_TYPE_JSON = 1,
    QUERY_TYPE_PARQUET = 2
} Query_Data_Type;

/* initialize the duckdb before running queries
** num_connection is the size of connection pool
** return 0 on success, negative value on error
*/
int query_init_db(int num_connection);

/* close the duckdb after use
*/
void query_close_db(void);

/* run the query on the duckdb
** input_format and output_format can be JSON, CSV, PARQUET
** if use_csv_headers_input is true, assumes the input contains column names if input_format is CSV,
** and the output will contain the column names if output_format is CSV
** query result is stored in result
** output_len is the length of the output
** return 0 on success, negative value on error
*/
int
run_query(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_length,
          char *sql, size_t *output_len, Query_Data_Type input_format,
          Query_Data_Type output_format, bool use_csv_headers_input,
          bool use_csv_headers_output, unsigned char **result);

#endif //QUERY_H
