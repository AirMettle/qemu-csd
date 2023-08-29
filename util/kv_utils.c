/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include "qemu/kv_utils.h"
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

const char *base_dir = NULL;

void kv_store_init(void) {
    base_dir = getenv("KV_BASE_DIR");
    if (!base_dir) {
        /* use current dir */
        base_dir = ".";
    }
}

void hex(const unsigned char *key, size_t key_len, char *buffer) {
    static const char hexout[] = "0123456789ABCDEF";
    for (size_t i = 0; i < key_len; ++i) {
        unsigned char c = key[i];
        buffer[2 * i] = hexout[c >> 4];
        buffer[2 * i + 1] = hexout[c & 0xF];
    }
}

const char *
get_path_str(uint32_t bus_number, uint32_t namespace_id, const unsigned char *key, size_t key_len,
             bool create_folder_on_absence) {
    size_t base_dir_len = strlen(base_dir);
    char *path_str = malloc(base_dir_len + 2 + 33 + 33 + 2 * key_len + 1);
    if (!path_str) {
        return NULL;
    }
    size_t pos = 0;
    strcpy(path_str, base_dir);
    pos += strlen(base_dir);
    if (create_folder_on_absence) mkdir(path_str, 0777);
    if (path_str[pos - 1] != '/') {
        path_str[pos++] = '/';
    }
    pos += sprintf(path_str + pos, "%u", bus_number);
    if (create_folder_on_absence) mkdir(path_str, 0777);
    path_str[pos++] = '/';

    pos += sprintf(path_str + pos, "%u", namespace_id);
    if (create_folder_on_absence) mkdir(path_str, 0777);
    path_str[pos++] = '/';

    if (key_len) { // get file path
        hex(key, key_len, path_str + pos);
        pos += 2 * key_len;
    } // else get dir path
    path_str[pos] = '\0';
    return path_str;
}