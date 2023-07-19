/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#ifndef KV_STORE_H
#define KV_STORE_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include "qemu/kv_utils.h"

/* returns number of bytes written, negative values on errors
If append is false, create new file overwriting and truncating if one already exists
If append is true, append to existing file.  If file does not exist, create it
*/
ssize_t store_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len,
                     unsigned char *value, size_t value_len, bool append, bool must_exist,
                     bool must_not_exist);

/* returns number of bytes read, negative values on errors
if offset is non-zero, begin reading at that offset
buffer is where the data should be read into
total_object_size is the total size of the object
*/
ssize_t read_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len,
                    size_t offset, unsigned char *buffer, size_t max_buffer_len,
                    size_t *total_object_size);

/* returns whether the file exists given a key.
 * 1 means file exists, 0 means file doesn't exist
 * negative values on errors
 */
int file_exist(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len);

/* return 0 on success */
int delete_object(uint32_t bus_number, uint32_t namespace_id, unsigned char *key, size_t key_len);

/* return keys in order that are greater or equal to key prefix
 * NULL on errors
 */
int list_objects(uint32_t bus_number, uint32_t namespace_id, unsigned char *key_prefix,
                        size_t key_prefix_len, size_t offset, size_t max_to_return,
                        size_t *num_objects_returned, ObjectKey **objects);

#endif //KV_STORE_H
