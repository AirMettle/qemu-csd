/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include <dirent.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "qemu/kv_utils.h"
#include "qemu/kv_store.h"

#define MIN(a, b) ((a) < (b) ? (a) : (b))


/* returns number of bytes written, -1 on error
If append is false, create new file overwriting and truncating if one already exists
If append is true, append to existing file or create file if it does not exist.
*/
ssize_t store_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len,
                     unsigned char *value, size_t value_len, bool append, bool must_exist,
                     bool must_not_exist) {
    if (must_exist && must_not_exist) {
        return KV_ERROR_INVALID_PARAMETER;
    }
    const char *path_str = get_path_str(bus_number, namespace_id, key, key_len, true);
    if (!path_str) {
        return KV_ERROR_FILE_PATH;
    }
    // test the file is already exist for append
    int exist = access(path_str, F_OK);
    if (must_exist && exist == -1) {
        free((void*)path_str);
        return KV_ERROR_FILE_NOT_FOUND;
    }
    if (must_not_exist && exist == 0) {
        free((void*)path_str);
        return KV_ERROR_FILE_EXISTS;
    }

    FILE *filePtr;
    if (append) filePtr = fopen(path_str, "ab");
    else filePtr = fopen(path_str, "wb");
    if (filePtr == NULL) {
        free((void*)path_str);
        return KV_ERROR_CANNOT_OPEN;
    }

    size_t elementsWritten = fwrite(value, sizeof(unsigned char), value_len, filePtr);

    fclose(filePtr);
    free((void*)path_str);
    if (elementsWritten != value_len) {
        return KV_ERROR_FILE_WRITE;
    }
    return elementsWritten;
}

/* returns number of bytes read, -1 on error
if offset is non-zero, begin reading at that offset
buffer is where the data should be read into
total_object_size is the total size of the object
*/
ssize_t read_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len,
                    size_t offset, unsigned char *buffer, size_t max_buffer_len,
                    size_t *total_object_size) {
    const char *path_str = get_path_str(bus_number, namespace_id, key, key_len, true);
    if (!path_str) return KV_ERROR_FILE_PATH;
    FILE *filePtr = fopen(path_str, "rb");
    if (filePtr == NULL) {
        free((void*)path_str);
        return KV_ERROR_CANNOT_OPEN;
    }

    fseek(filePtr, 0, SEEK_END);
    *total_object_size = ftell(filePtr);

    if (fseek(filePtr, offset, SEEK_SET) != 0) {
        fclose(filePtr);
        free((void*)path_str);
        return KV_ERROR_FILE_OFFSET;
    }

    size_t bytesRead = fread(buffer, sizeof(unsigned char), max_buffer_len, filePtr);
    free((void*)path_str);
    fclose(filePtr);
    return bytesRead;
}

/* return 0 on success */
int delete_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len) {
    const char *path_str = get_path_str(bus_number, namespace_id, key, key_len, true);
    if (!path_str) return KV_ERROR_FILE_PATH;
    int res = remove(path_str);
    free((void*)path_str);
    if (!res) {
        return 0;
    }
    if (errno == ENOENT) {
       return KV_ERROR_FILE_NOT_FOUND;
    }
    return KV_ERROR_REMOVE;
}

int compareStrings(const void *a, const void *b);
int compareStrings(const void *a, const void *b) {
    const char *str1 = *(const char **) a;
    const char *str2 = *(const char **) b;
    return strcmp(str1, str2);
}

unsigned char hex_str_to_uchar(char hex);
unsigned char hex_str_to_uchar(char hex) {
    if (hex <= '9') return hex - '0';
    return hex - 'A' + 10;
}

/* return keys in order that are greater or equal to key prefix*/
int list_objects(uint32_t bus_number, uint32_t namespace_id, unsigned char *key_prefix,
                        size_t key_prefix_len, size_t offset, size_t max_to_return,
                        size_t *num_objects_returned, ObjectKey **objects) {

    if (!max_to_return) {
        max_to_return = 0xFFFFFFFF;
    }
    DIR *dir = opendir(get_path_str(bus_number, namespace_id, NULL, 0, true));
    struct dirent *entry;
    if (dir == NULL) {
        return KV_ERROR_FILE_PATH;
    }

    char prefix_hex_str[2 * key_prefix_len + 1];
    hex(key_prefix, key_prefix_len, prefix_hex_str);
    prefix_hex_str[2 * key_prefix_len] = '\0';

    // get all keys in string that are >= prefix
    size_t dir_list_size = 0, dir_list_capacity = 128, max_key_str_len = 2 * 16 + 1;
    char **dir_list = malloc(dir_list_capacity * sizeof(char *));
    if (!dir_list) {
        closedir(dir);
        return KV_ERROR_MEMORY_ALLOCATION;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (dir_list_size == dir_list_capacity) {
            dir_list_capacity = MIN(dir_list_capacity * 2, max_to_return + offset);
            char **new_dir_list = realloc(dir_list, dir_list_capacity * sizeof(char *));
            if (!new_dir_list) {
                free(dir_list);
                closedir(dir);
                return KV_ERROR_MEMORY_ALLOCATION;
            }
            dir_list = new_dir_list;
        }
        if (entry->d_type != DT_REG) continue;
        if (key_prefix_len && strcmp(entry->d_name, prefix_hex_str) < 0) continue;
        dir_list[dir_list_size] = malloc(max_key_str_len);
        if (!dir_list[dir_list_size]) {
            for (int i = 0; i < dir_list_size; ++i) {
                free(dir_list[i]);
            }
            free(dir_list);
            closedir(dir);
            return KV_ERROR_MEMORY_ALLOCATION;
        }
        strcpy(dir_list[dir_list_size++], entry->d_name);
    }
    closedir(dir);
    if (dir_list_size <= offset) {
        for (int i = 0; i < dir_list_size; ++i) {
            free(dir_list[i]);
        }
        free(dir_list);
        *num_objects_returned = 0;
        *objects = NULL;
        return 0;
    }

    // sort the keys
    qsort(dir_list, dir_list_size, sizeof(char *), compareStrings);

    // convert string to unsigned char and put into ObjectKey list
    *num_objects_returned = MIN(dir_list_size - offset, max_to_return);
    *objects = malloc((*num_objects_returned) * sizeof(ObjectKey));
    if (!(*objects)) {
        for (int i = 0; i < dir_list_size; ++i) {
            free(dir_list[i]);
        }
        free(dir_list);
        return KV_ERROR_MEMORY_ALLOCATION;
    }

    for (size_t i = offset; i < offset + (*num_objects_returned); ++i) {
        size_t key_len = strlen(dir_list[i]) / 2;
        (*objects)[i - offset].key_len = key_len;
        if (key_len > 16) {
            for (int j = 0; j < dir_list_size; ++j) {
                free(dir_list[j]);
            }
            free(dir_list);
            free(*objects);
            return KV_ERROR_KEY_TOO_LONG;
        }
        for (int j = 0; j < key_len; ++j) {
            (*objects)[i - offset].key[j] =
                    hex_str_to_uchar(dir_list[i][j * 2]) * 16 +
                    hex_str_to_uchar(dir_list[i][j * 2 + 1]);
        }
    }

    for (int i = 0; i < dir_list_size; ++i) {
        free(dir_list[i]);
    }
    free(dir_list);
    return 0;
}

/* returns whether the file exists given a key.
 * 1 means file exists, 0 means file doesn't exist
 * negative values on errors
 */
int file_exist(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len) {
    const char *path_str = get_path_str(bus_number, namespace_id, key, key_len, false);
    if (!path_str) return KV_ERROR_FILE_PATH;
    // test the file is already exist for append
    return access(path_str, F_OK) + 1;
}
