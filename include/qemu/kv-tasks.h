/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Chia-Lin Wu <cwu@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#ifndef KV_TASK_H
#define KV_TASK_H

#include "qemu/osdep.h"
#include "qemu/queue.h"
#include "qemu/event_notifier.h"
#include "qemu/query.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define KV_TASK_KEY_MAX_LENGTH 16

typedef enum kv_task_type {
    KV_TASK_STORE,
    KV_TASK_RETRIEVE,
    KV_TASK_LIST,
    KV_TASK_DELETE,
    KV_TASK_EXISTS,
    KV_TASK_SEND_SELECT
} kv_task_type;

typedef struct kv_task_request {
    kv_task_type task_type;
    uint32_t bus_number;
    uint32_t namespace_id;
    void *nvme_cmd;
    unsigned char key[KV_TASK_KEY_MAX_LENGTH];
    size_t key_length;
    unsigned char *data;
    size_t data_length;
    size_t max_length;
    bool must_exist;
    bool must_not_exist;
    bool append;
    size_t offset;
    Query_Data_Type select_input_type;
    Query_Data_Type select_output_type;
    bool use_csv_headers_input;
    bool use_csv_headers_output;
    QSIMPLEQ_ENTRY(kv_task_request) request_list;
} kv_task_request;

typedef struct kv_task_result {
    kv_task_type task_type;
    void *nvme_cmd;
    ssize_t status;
    void *result;
    size_t result_length;
    size_t max_length;
    QSIMPLEQ_ENTRY(kv_task_result) result_list;
} kv_task_result;

int kv_tasks_add_request_with_params(kv_task_type task_type, uint32_t bus_number, uint32_t namespace_id,
    void *nvme_cmd, unsigned char *key, size_t key_length, unsigned char *data, size_t data_length,
    size_t max_length, bool must_exist, bool must_not_exist, bool append, size_t offset,
    Query_Data_Type select_input_type, Query_Data_Type select_output_type,
    bool use_csv_headers_input, bool use_csv_headers_output);
void kv_tasks_add_request(kv_task_request *request);
kv_task_result *kv_tasks_get_next_result(void);
void kv_tasks_init(EventNotifier *event_notifier);
void kv_tasks_free_result(kv_task_result *result);

#endif
