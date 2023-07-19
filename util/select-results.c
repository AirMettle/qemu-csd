/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Chia-Lin Wu <cwu@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include "qemu/select-results.h"
#include "qemu/osdep.h"
#include "qemu/main-loop.h"

#define SELECT_NUM_CACHE_ENTRIES 32

typedef struct select_store_data_entry {
    unsigned char *data;
    size_t data_len;
    uint32_t id;
    uint32_t last_id;
    bool in_use;
} select_store_data_entry;

static select_store_data_entry data_cache[SELECT_NUM_CACHE_ENTRIES];
static uint32_t next_id;
static QemuMutex select_mutex;
static bool init;

void select_results_init(void) {
    select_store_data_entry *entry;
    if (init) {
        return;
    }
    init = true;
    qemu_mutex_init(&select_mutex);
    for (int i = 0; i < SELECT_NUM_CACHE_ENTRIES; i++) {
        entry = &data_cache[i];
        entry->last_id = i;
    }
}

uint32_t select_results_store(unsigned char *results, size_t results_len) {
    select_store_data_entry *entry;
    select_store_data_entry *oldest_entry = NULL;

    qemu_mutex_lock(&select_mutex);
    for (int i = 0; i < SELECT_NUM_CACHE_ENTRIES; i++) {
        entry = &data_cache[next_id];
        if (!entry->in_use) {
            entry->in_use = true;
            entry->data = results;
            entry->data_len = results_len;
            entry->id = entry->last_id + SELECT_NUM_CACHE_ENTRIES;
            uint32_t id = entry->id;
	    next_id = (next_id + 1) % SELECT_NUM_CACHE_ENTRIES;
            qemu_mutex_unlock(&select_mutex);
            return id;
        }
        if (!oldest_entry || (oldest_entry->id > entry->id)) {
            oldest_entry = entry;
        }
	next_id = (next_id + 1) % SELECT_NUM_CACHE_ENTRIES;
    }

    // nothing empty, use oldest
    if (oldest_entry->data) {
        g_free(oldest_entry->data);
    }
    oldest_entry->data = results;
    oldest_entry->data_len = results_len;
    oldest_entry->id = oldest_entry->id + SELECT_NUM_CACHE_ENTRIES;
    uint32_t id = oldest_entry->id;
    qemu_mutex_unlock(&select_mutex);
    return id;
}

unsigned char *select_results_retrieve(uint32_t id, size_t *data_len,
                                       bool do_not_remove,
                                       bool do_not_remove_if_size_gt,
                                       size_t size_check, bool *found) {
    *data_len = 0;
    *found = false;
    qemu_mutex_lock(&select_mutex);
    select_store_data_entry *entry = &data_cache[id % SELECT_NUM_CACHE_ENTRIES];
    if (!entry->data || (entry->id != id)) {
        qemu_mutex_unlock(&select_mutex);
        return NULL;
    }
    unsigned char *data = entry->data;
    *data_len = entry->data_len;
    if (!do_not_remove && (!do_not_remove_if_size_gt || (entry->data_len <= size_check))) {
        entry->data_len = 0;
        entry->data = NULL;
        entry->last_id = entry->id;
        entry->id = 0;
        entry->in_use = false;
    } else {
        unsigned char *data_copy = g_malloc(entry->data_len);
        memcpy(data_copy, data, entry->data_len);
        data = data_copy;
    }
    qemu_mutex_unlock(&select_mutex);
    *found = true;
    return data;
}
