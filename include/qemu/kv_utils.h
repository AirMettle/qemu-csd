/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#ifndef KV_UTILS_H
#define KV_UTILS_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

#define KV_ERROR_INVALID_PARAMETER (-1)
#define KV_ERROR_FILE_PATH (-2)
#define KV_ERROR_FILE_EXISTS (-3)
#define KV_ERROR_FILE_NOT_FOUND (-4)
#define KV_ERROR_CANNOT_OPEN (-5)
#define KV_ERROR_FILE_WRITE (-6)
#define KV_ERROR_FILE_OFFSET (-7)
#define KV_ERROR_QUERY (-8)
#define KV_ERROR_FILE_READ (-9)
#define KV_ERROR_MEMORY_ALLOCATION (-10)
#define KV_ERROR_PIPE (-11)
#define KV_ERROR_FORK (-12)
#define KV_ERROR_DUCKDB (-13)
#define KV_ERROR_REMOVE (-14)
#define KV_ERROR_KEY_TOO_LONG (-15)

typedef struct ObjectKey {
    unsigned char key[16];
    size_t key_len;
} ObjectKey;

void kv_store_init(void);

void hex(const unsigned char *key, size_t key_len, char *buffer);

const char *
get_path_str(uint32_t bus_number, uint32_t namespace_id, const unsigned char *key, size_t key_len,
             bool create_folder_on_absence);

#endif //KV_UTILS_H
