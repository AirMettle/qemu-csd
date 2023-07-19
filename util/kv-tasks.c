/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Chia-Lin Wu <cwu@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include "qemu/kv-tasks.h"
#include "qemu/kv_store.h"
#include "qemu/main-loop.h"
#include "qemu/thread.h"
#include "qemu/query.h"

#define KV_TASK_NUM_THREADS 5
#define KV_TASK_NUM_DB_CONNS 5

static QSIMPLEQ_HEAD(, kv_task_request) requests =
    QSIMPLEQ_HEAD_INITIALIZER(requests);
static QSIMPLEQ_HEAD(, kv_task_result) results =
    QSIMPLEQ_HEAD_INITIALIZER(results);

static QemuMutex requests_mutex;
static QemuMutex results_mutex;
static QemuThread *task_threads;
static int num_threads;
static int num_db_conns;
static EventNotifier *notifier;
static QemuCond tasks_cond;
static bool init;

static void *kv_tasks_run_thread(void *opaque);

void kv_tasks_init(EventNotifier *event_notifier) {
    if (init) {
        return;
    }
    init = true;
    assert(qemu_in_main_thread());
    notifier = event_notifier;
    qemu_mutex_init(&requests_mutex);
    qemu_mutex_init(&results_mutex);
    qemu_cond_init(&tasks_cond);
    kv_store_init();

    const char *num_threads_env = getenv("KV_NUM_THREADS");
    if (num_threads_env) {
        num_threads = atoi(num_threads_env);
    }
    if (num_threads <= 0 || num_threads > 1024) {
        num_threads = KV_TASK_NUM_THREADS;
    }
    task_threads = g_new0(QemuThread, num_threads);
    for (int i = 0; i < num_threads; i++) {
        qemu_thread_create(task_threads + i, "kv_task", kv_tasks_run_thread,
                           NULL, QEMU_THREAD_DETACHED);
    }

    const char *num_db_conns_env = getenv("KV_NUM_DB_CONNS");
    if (num_db_conns_env) {
        num_db_conns = atoi(num_db_conns_env);
    }
    if (num_db_conns <= 0 || num_db_conns > 256) {
        num_db_conns = KV_TASK_NUM_DB_CONNS;
    }
    query_init_db(num_db_conns);
}

int kv_tasks_add_request_with_params(kv_task_type task_type, uint32_t bus_number, uint32_t namespace_id,
    void *nvme_cmd, unsigned char *key, size_t key_length, unsigned char *data, size_t data_length,
    size_t max_length, bool must_exist, bool must_not_exist, bool append, size_t offset,
    Query_Data_Type select_input_type, Query_Data_Type select_output_type,
    bool use_csv_headers_input, bool use_csv_headers_output) {
    kv_task_request *request = g_new0(kv_task_request, 1);
    if (!request) {
         return -1;
    }
    request->task_type = task_type;
    request->bus_number = bus_number;
    request->namespace_id = namespace_id;
    request->nvme_cmd = nvme_cmd;
    if (key_length && key_length <= KV_TASK_KEY_MAX_LENGTH) {
       memcpy(request->key, key, key_length);
    }
    request->key_length = key_length;
    request->data = data;
    request->data_length = data_length;
    request->max_length = max_length;
    request->must_exist = must_exist;
    request->must_not_exist = must_not_exist;
    request->append = append;
    request->offset = offset;
    request->select_input_type = select_input_type;
    request->select_output_type = select_output_type;
    request->use_csv_headers_input = use_csv_headers_input;
    request->use_csv_headers_output = use_csv_headers_output;
    kv_tasks_add_request(request);
    return 0;
}

void kv_tasks_add_request(kv_task_request *request) {
    qemu_mutex_lock(&requests_mutex);
    QSIMPLEQ_INSERT_TAIL(&requests, request, request_list);
    qemu_mutex_unlock(&requests_mutex);
    qemu_cond_signal(&tasks_cond);
}

kv_task_result *kv_tasks_get_next_result(void) {
    assert(qemu_in_main_thread());
    qemu_mutex_lock(&results_mutex);

    kv_task_result *result = QSIMPLEQ_FIRST(&results);
    if (result) {
        QSIMPLEQ_REMOVE_HEAD(&results, result_list);
    }
    qemu_mutex_unlock(&results_mutex);
    return result;
}

void kv_tasks_free_result(kv_task_result *result) {
    if (result->result) {
        g_free(result->result);
    }
    g_free(result);
}

static void kv_tasks_send_result(kv_task_request *request, ssize_t status,
                                 void *result_data, size_t result_data_length,
                                 size_t max_length) {
    kv_task_result *result;

    result = g_new0(kv_task_result, 1);
    result->task_type = request->task_type;
    result->nvme_cmd = request->nvme_cmd;
    result->status = status;
    result->result = result_data;
    result->result_length = result_data_length;
    result->max_length = max_length;
    qemu_mutex_lock(&results_mutex);
    QSIMPLEQ_INSERT_TAIL(&results, result, result_list);
    qemu_mutex_unlock(&results_mutex);

    if (request->data) {
        g_free(request->data);
    }
    g_free(request);
    event_notifier_set(notifier);
}

static void *kv_tasks_run_thread(void *opaque) {
    while (1) {
        qemu_mutex_lock(&requests_mutex);
        kv_task_request *request = QSIMPLEQ_FIRST(&requests);
        if (request) {
            QSIMPLEQ_REMOVE_HEAD(&requests, request_list);
        } else {
            qemu_cond_wait(&tasks_cond, &requests_mutex);
            request = QSIMPLEQ_FIRST(&requests);
            if (request) {
                QSIMPLEQ_REMOVE_HEAD(&requests, request_list);
            }
        }
        qemu_mutex_unlock(&requests_mutex);
        if (!request) {
            continue;
        }
        ssize_t status = -1;
        size_t result_data_length = 0;
        size_t max_length = 0;
        void *result_data = NULL;
        switch (request->task_type) {
        case KV_TASK_STORE: {
            status = store_object(
                request->bus_number, request->namespace_id, request->key,
                request->key_length, request->data, request->data_length,
                request->append, request->must_exist, request->must_not_exist);
        } break;
        case KV_TASK_RETRIEVE: {
            unsigned char *buffer = g_malloc(request->max_length);
            if (!buffer) {
                break;
            }
            size_t total_size = 0;
            status =
                read_object(request->bus_number, request->namespace_id,
                            request->key, request->key_length, request->offset,
                            buffer, request->max_length, &total_size);
            if (status > 0) {
                result_data = (void *)buffer;
                result_data_length = request->max_length < total_size
                                         ? request->max_length
                                         : total_size;
                max_length = total_size;
            } else {
                g_free(buffer);
            }
        } break;
        case KV_TASK_LIST: {
            ObjectKey *list;
            size_t num_objects = 0;
            status =
                list_objects(request->bus_number, request->namespace_id,
                             request->key, request->key_length, request->offset,
                             request->max_length, &num_objects, &list);
            result_data_length = num_objects;
            result_data = (void *)list;
        } break;
        case KV_TASK_DELETE: {
            status = (ssize_t)delete_object(request->bus_number,
                                            request->namespace_id, request->key,
                                            request->key_length);
        } break;
        case KV_TASK_EXISTS: {
            status =
                (ssize_t)file_exist(request->bus_number, request->namespace_id,
                                    request->key, request->key_length);
        } break;
        case KV_TASK_SEND_SELECT: {
            size_t output_len;
            unsigned char *result;
            status = run_query(request->bus_number, request->namespace_id, request->key, request->key_length,
                               (char *) request->data, &output_len, request->select_input_type, request->select_output_type,
                                request->use_csv_headers_input, request->use_csv_headers_output, &result);
            if (status == 0) {
                result_data = (void *) result;
                result_data_length = output_len;
            }
        } break;

        default:
            status = -1;
            break;
        }
        kv_tasks_send_result(request, status, result_data, result_data_length,
                             max_length);
    }
}
