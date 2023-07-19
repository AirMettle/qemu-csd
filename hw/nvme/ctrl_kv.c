/*
 * QEMU NVM Express Controller for KV Storage
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Authors:
 *  Chia-Lin Wu <cwu@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */

#include "qemu/select-results.h"
#include "qemu/kv-tasks.h"
#include "qemu/kv_store.h"

#include "nvme.h"

#define NVME_KV_MAX_LEN_LENGTH 16

static void nvme_kv_notifier(EventNotifier *e);

void nvme_kv_init(NvmeCtrl *n) {
    event_notifier_init(&n->kv_notifier, 0);
    event_notifier_set_handler(&n->kv_notifier, nvme_kv_notifier);
    kv_tasks_init(&n->kv_notifier); 
    select_results_init();
}

static int nvme_kv_get_key(NvmeKvCmd *cmd, unsigned char *key_buf, size_t *key_len, bool empty_allowed) {
    size_t kv_length = NVME_KV_GET_KEY_LENGTH(cmd->key_length_and_options);

    if ((!empty_allowed && kv_length == 0) || kv_length > 16) {
        return -1;
    }

    if (kv_length == 0) {
        *key_len = 0;
        return 0;
    }

    uint32_t words[4];
    words[0] = le32_to_cpu(cmd->key_word_4);
    words[1] = le32_to_cpu(cmd->key_word_3);
    words[2] = le32_to_cpu(cmd->key_word_2);
    words[3] = le32_to_cpu(cmd->key_word_1);

    unsigned char *p = key_buf;
    size_t len = kv_length;
    for (int i=0; i<4 && (len > 0); i++) {
        for (int j=3; j>=0 && (len > 0); j--, len--, p++) {
            *p = (words[i] >> (8 * j)) & 0xFF;
        }
    }

    *key_len = kv_length;
    return 0;
}


static size_t write_data_to_QEMUSGList(QEMUSGList *sglist, const unsigned char *data, size_t data_len) {
    size_t remaining = data_len;
    size_t offset = 0;
    size_t data_offset = 0;
    int sg_index = 0;

    while (remaining > 0 && sg_index < sglist->nsg) {
        ScatterGatherEntry *current_sg = &sglist->sg[sg_index];

        size_t sg_remaining = current_sg->len - offset;
        dma_addr_t bytes_to_copy = (remaining < sg_remaining) ? remaining : sg_remaining;

        dma_addr_t dma_addr = current_sg->base + offset;
        void *virt_addr = dma_memory_map(sglist->as, dma_addr, &bytes_to_copy, DMA_DIRECTION_FROM_DEVICE, MEMTXATTRS_UNSPECIFIED);
 
        if (virt_addr && bytes_to_copy) {
            memcpy(virt_addr, (char *)data + data_offset, bytes_to_copy);
        } else {
            // Handle error case when mapping failed or not all data could be mapped.
        }       
 
        if (virt_addr) {
            dma_memory_unmap(sglist->as, virt_addr, bytes_to_copy, DMA_DIRECTION_FROM_DEVICE, bytes_to_copy);
        }       
 
        remaining -= bytes_to_copy;
        offset += bytes_to_copy;
        data_offset += bytes_to_copy;

        if (offset == current_sg->len) {
            offset = 0;
            sg_index++;
        }
    }
    return data_len - remaining;
}

static size_t read_data_from_QEMUSGList(const QEMUSGList *sglist, unsigned char *buffer, size_t buffer_len) {
    size_t remaining = buffer_len;
    size_t offset = 0;
    size_t buffer_offset = 0;
    int sg_index = 0;

    while (remaining > 0 && sg_index < sglist->nsg) {
        const ScatterGatherEntry *current_sg = &sglist->sg[sg_index];
        size_t sg_remaining = current_sg->len - offset;
        dma_addr_t bytes_to_copy = (remaining < sg_remaining) ? remaining : sg_remaining;

        dma_addr_t dma_addr = current_sg->base + offset;
        void *virt_addr = dma_memory_map(sglist->as, dma_addr, &bytes_to_copy, DMA_DIRECTION_TO_DEVICE, MEMTXATTRS_UNSPECIFIED);

        if (virt_addr && bytes_to_copy) {
            memcpy((char *)buffer + buffer_offset, virt_addr, bytes_to_copy);
        } else {
            // Handle error case when mapping failed or not all data could be mapped.
        }

        if (virt_addr) {
            dma_memory_unmap(sglist->as, virt_addr, bytes_to_copy, DMA_DIRECTION_TO_DEVICE, bytes_to_copy);
        }

        remaining -= bytes_to_copy;
        offset += bytes_to_copy;
        buffer_offset += bytes_to_copy;
        if (offset == current_sg->len) {
            offset = 0;
            sg_index++;
        }
    }
    return buffer_len - remaining;
}

static size_t write_data_to_QEMUIOVector(QEMUIOVector *iov, const unsigned char *data, size_t data_len) {
    size_t remaining = data_len;
    size_t offset = 0;

    for (int i = 0; i < iov->niov; i++) {
        size_t iovLen = iov->iov[i].iov_len;

        if (remaining <= iovLen) {
            memcpy(iov->iov[i].iov_base, (const uint8_t*)data + offset, remaining);
            break;
        } else {
            memcpy(iov->iov[i].iov_base, (const uint8_t*)data + offset, iovLen);
            remaining -= iovLen;
            offset += iovLen;
        }
    }
    return data_len - remaining;
}

static size_t read_data_from_QEMUIOVector(const QEMUIOVector *iov, unsigned char *buffer, size_t buffer_len) {
    size_t remaining = buffer_len;
    size_t offset = 0;

    for (int i = 0; i < iov->niov; i++) {
        size_t iovLen = iov->iov[i].iov_len;

        if (remaining <= iovLen) {
            memcpy((uint8_t*)buffer + offset, iov->iov[i].iov_base, remaining);
            break;
        } else {
            memcpy((uint8_t*)buffer + offset, iov->iov[i].iov_base, iovLen);
            remaining -= iovLen;
            offset += iovLen;
        }
    }
    return buffer_len - remaining;
}

static size_t nvme_kv_write_data(NvmeRequest *req, unsigned char *data, size_t data_len) {
    if (req->sg.flags & NVME_SG_DMA) {
        return write_data_to_QEMUSGList(&req->sg.qsg, data, data_len);
    } else {
        return write_data_to_QEMUIOVector(&req->sg.iov, data, data_len);
    }
}

static size_t nvme_kv_read_data(NvmeRequest *req, unsigned char *buffer, size_t buffer_len) {
    if (req->sg.flags & NVME_SG_DMA) {
        return read_data_from_QEMUSGList(&req->sg.qsg, buffer, buffer_len);
    } else {
        return read_data_from_QEMUIOVector(&req->sg.iov, buffer, buffer_len);
    }
}

static uint16_t nvme_build_kv_list_response(ObjectKey *keys, size_t num_keys,
                                            unsigned char *list_buffer, size_t max_list_buffer_size,
                                            size_t *list_buffer_size) {
    size_t pad;
    ObjectKey *key;
    size_t key_length;
    size_t key_length_le;
    unsigned char *key_length_ptr;
    size_t num_keys_written = 0;

    unsigned char *p = list_buffer;
    *list_buffer_size = 0;

    if (max_list_buffer_size < 4) {
        return NVME_CMD_SIZE_LIMIT;
    }

    key_length_ptr = p;
    p += 4;
    max_list_buffer_size -= 4;


    for (size_t i=0; i<num_keys; i++) {
        key = &keys[i];
        key_length = key->key_len;
        key_length_le = cpu_to_le16(key->key_len);
        pad = (4 - (key_length % 4)) % 4;
        if (max_list_buffer_size < (2 + key_length + pad)) {
            break;
        }
        memcpy(p, &key_length_le, 2);
        p += 2;
        memcpy(p, key->key, key_length);
        p += key_length;
        if (pad) {
            memset(p, 0, pad);
            p += pad;
        }
        max_list_buffer_size -= (2 + key_length + pad);
        num_keys_written++;
    }

    uint32_t len_le = cpu_to_le32(num_keys_written);
    memcpy(key_length_ptr, &len_le, 4);

    *list_buffer_size = p - list_buffer;
    return NVME_SUCCESS;
}

static uint16_t nvme_kv_list(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;

    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, true)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }
    size_t max_len = le32_to_cpu(kv->host_buffer_size);
    uint16_t status = nvme_map_dptr(n, &req->sg, max_len, &req->cmd);
    if (status != NVME_SUCCESS) {
        return status | NVME_DNR;
    }

    kv_tasks_add_request_with_params(KV_TASK_LIST, pci_dev_bus_num(&n->parent_obj), le32_to_cpu(req->cmd.nsid),
         req, key, key_length, NULL, 0, 0, false, false, false, 0, 0, 0, false, false);

    return NVME_NO_COMPLETE;
}

static uint16_t nvme_kv_exist(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;

    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, false)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }

    kv_tasks_add_request_with_params(KV_TASK_EXISTS, pci_dev_bus_num(&n->parent_obj),
        le32_to_cpu(req->cmd.nsid),
       req, key, key_length, NULL, 0, 0, false, false, false, 0, 0, 0, false, false);

    return NVME_NO_COMPLETE;
}

static uint16_t nvme_kv_delete(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;

    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, false)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }

    kv_tasks_add_request_with_params(KV_TASK_DELETE, pci_dev_bus_num(&n->parent_obj), le32_to_cpu(req->cmd.nsid),
        req, key, key_length, NULL, 0, 0, false, false, false, 0, 0, 0, false, false);

    return NVME_NO_COMPLETE;
}

static uint16_t nvme_kv_store(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;

    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, false)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }

    uint8_t store_options = NVME_KV_GET_CMD_OPTIONS(kv->key_length_and_options);
    bool must_exist = NVME_STORE_CMD_OPTION_MUST_EXIST(store_options);
    bool must_not_exist = NVME_STORE_CMD_OPTION_MUST_NOT_EXIST(store_options);
    bool append = NVME_STORE_CMD_OPTION_APPEND(store_options);
    size_t value_size = le32_to_cpu(kv->host_buffer_size);
    uint16_t status = nvme_map_dptr(n, &req->sg, value_size, &req->cmd);
    if (status != NVME_SUCCESS) {
        return status | NVME_DNR;
    }
    unsigned char *buffer = g_malloc0(value_size);
    if (!buffer && value_size) {
        return NVME_KV_ERROR | NVME_DNR;
    }

    size_t bytes_read = nvme_kv_read_data(req, buffer, value_size);
    if (bytes_read != value_size) {
        // no error is returned if there is less data than host buffer size
    }
    kv_tasks_add_request_with_params(KV_TASK_STORE, pci_dev_bus_num(&n->parent_obj), le32_to_cpu(req->cmd.nsid),
       req, key, key_length, buffer, value_size, 0, must_exist, must_not_exist, append, 0, 0, 0, false, false);

    return NVME_NO_COMPLETE;
}

static uint16_t nvme_kv_retrieve(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;
    uint16_t status;
 
    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, false)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }

    size_t max_len = le32_to_cpu(kv->host_buffer_size);
    size_t offset = le32_to_cpu(kv->read_offset);
    status = nvme_map_dptr(n, &req->sg, max_len, &req->cmd);
    if (status != NVME_SUCCESS) {
        return status | NVME_DNR;
    }

    kv_tasks_add_request_with_params(KV_TASK_RETRIEVE, pci_dev_bus_num(&n->parent_obj), le32_to_cpu(req->cmd.nsid),
       req, key, key_length, NULL, 0, max_len, false, false, false, offset, 0, 0, false, false);

    return NVME_NO_COMPLETE;
}

static Query_Data_Type nvme_select_type_to_data_type(uint8_t select_type, bool *found) {
    *found = true;
    switch (select_type) {
        case NVME_SELECT_TYPE_CSV:
            return QUERY_TYPE_CSV;
        case NVME_SELECT_TYPE_JSON:
            return QUERY_TYPE_JSON;
        case NVME_SELECT_TYPE_PARQUET:
            return QUERY_TYPE_PARQUET;
        default:
            *found = false;
            return QUERY_TYPE_CSV;
    }
}

static uint16_t nvme_kv_send_select(NvmeCtrl *n, NvmeRequest *req) {
    unsigned char key[NVME_KV_MAX_LEN_LENGTH];
    size_t key_length;
    uint16_t status;
    bool found;

    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    if (nvme_kv_get_key(kv, key, &key_length, false)) {
        return NVME_INVALID_KV_SIZE | NVME_DNR;
    }

    Query_Data_Type input_type = nvme_select_type_to_data_type(NVME_SELECT_CMD_INPUT_TYPE(kv->key_length_and_options), &found);
    if (!found) {
        return NVME_KV_INVALID_PARAMETER | NVME_DNR;
    }

    Query_Data_Type output_type = nvme_select_type_to_data_type(NVME_SELECT_CMD_OUTPUT_TYPE(kv->key_length_and_options), &found);
    if (!found) {
        return NVME_KV_INVALID_PARAMETER | NVME_DNR;
    }

    uint8_t select_options = NVME_KV_GET_CMD_OPTIONS(kv->key_length_and_options);
    bool use_csv_headers_input = NVME_SELECT_CMD_OUTPUT_TYPE_USE_CSV_HEADERS_INPUT(select_options);
    bool use_csv_headers_output = NVME_SELECT_CMD_OUTPUT_TYPE_USE_CSV_HEADERS_OUTPUT(select_options);

    size_t len = le32_to_cpu(kv->host_buffer_size);
    status = nvme_map_dptr(n, &req->sg, len, &req->cmd);
    if (status != NVME_SUCCESS) {
        return status | NVME_DNR;
    }

    unsigned char *buffer = g_malloc0(len + 1);
    if (!buffer) {
        return NVME_KV_ERROR | NVME_DNR;
    }
    size_t bytes_read = nvme_kv_read_data(req, buffer, len);
    buffer[bytes_read] = '\0';

    kv_tasks_add_request_with_params(KV_TASK_SEND_SELECT, pci_dev_bus_num(&n->parent_obj), le32_to_cpu(req->cmd.nsid),
        req, key, key_length, buffer, bytes_read + 1, 0, false, false, false, 0, input_type, output_type,
        use_csv_headers_input, use_csv_headers_output);

    return NVME_NO_COMPLETE;
}

static uint16_t nvme_kv_retrieve_select(NvmeCtrl *n, NvmeRequest *req) {
    uint16_t status;
    
    NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
    size_t max_len = le32_to_cpu(kv->host_buffer_size);
    uint8_t select_options = le32_to_cpu(kv->key_length_and_options);
    bool do_not_free = NVME_SELECT_CMD_OPTION_DO_NOT_FREE(select_options);
    bool do_not_free_if_not_all_data_fetched = NVME_SELECT_CMD_OPTION_DO_NOT_FREE_IF_NOT_ALL_DATA_FETCHED(select_options);
    size_t offset = le32_to_cpu(kv->read_offset);
   
    uint32_t select_id = kv->select_id;
    size_t results_len;
    bool found;
    unsigned char *results = select_results_retrieve(select_id, &results_len, do_not_free, do_not_free_if_not_all_data_fetched, max_len + offset, &found);
    if (!found) {
        return  NVME_KV_NOT_FOUND | NVME_DNR;
    }

    status = nvme_map_dptr(n, &req->sg, max_len, &req->cmd);
    if (status != NVME_SUCCESS) {
        if (results) {
           g_free(results);
        }
        return status | NVME_DNR;
    }
   
    size_t return_len = results_len > offset ? (results_len - offset) : 0;
    if (return_len) {
        size_t bytes_written = nvme_kv_write_data(req, results + offset, return_len < max_len ? return_len : max_len);
        if (bytes_written != results_len) {
           // no error is returned if there is not enough room in buffer
        }
    }

    if (results) {
       g_free(results);
    }

    /* total data size */
    req->cqe.result = cpu_to_le32(results_len);
    return NVME_SUCCESS;
}

static void nvme_kv_notifier(EventNotifier *e) {
    kv_task_result *result;

    event_notifier_test_and_clear(e);
    while ((result = kv_tasks_get_next_result())) {
        NvmeRequest *req = (NvmeRequest *) result->nvme_cmd;
        NvmeKvCmd *kv = (NvmeKvCmd *)&req->cmd;
        uint16_t cqe_status = NVME_SUCCESS;
        uint32_t cqe_result = 0;

        switch (result->task_type) {
            case KV_TASK_STORE: 
                if (result->status < 0) {
                    if (result->status == KV_ERROR_FILE_NOT_FOUND) {
                       cqe_status = NVME_KV_NOT_FOUND;
                    } else if (result->status == KV_ERROR_FILE_EXISTS) {
                       cqe_status = NVME_KV_EXISTS;
                    } else {
                       cqe_status = NVME_KV_ERROR;
                    }
                }
                break;
            case KV_TASK_DELETE:
                if (result->status < 0) {
                    if (result->status == KV_ERROR_FILE_NOT_FOUND) {
                        cqe_status = NVME_KV_NOT_FOUND;
                    } else {
                        cqe_status = NVME_KV_ERROR;
                    }
                }
                break;
            case KV_TASK_EXISTS:
                if (result->status != 1) {
                    cqe_status = NVME_KV_NOT_FOUND;
                }
                break;
            case KV_TASK_RETRIEVE:
                {
                    if (result->status < 0) {
                        cqe_status = result->status == KV_ERROR_CANNOT_OPEN ? NVME_KV_NOT_FOUND : NVME_KV_ERROR;
                    } else {
                        size_t len = le32_to_cpu(kv->host_buffer_size);
                        size_t bytes_written = nvme_kv_write_data(req, (unsigned char *) result->result,
                                                                  result->result_length < len ? result->result_length: len);
                        if (bytes_written != result->result_length) {
                           // no error is returned if there is not enough room in buffer
                        }            
                        cqe_result = result->max_length;
                    }
                }
                break;
            case KV_TASK_LIST:
                {
                    if (result->status < 0) {
                        cqe_status = NVME_KV_ERROR;
                    } else {
                        size_t len = le32_to_cpu(kv->host_buffer_size);
                        ObjectKey *keys = (ObjectKey *) result->result;
                        size_t num_keys = result->result_length;
                        unsigned char *list_buffer = g_malloc(len);
                        if (!list_buffer) {
                            cqe_status = NVME_KV_ERROR;
                        } else {
                            size_t list_buffer_size;
                            uint16_t status = nvme_build_kv_list_response(keys, num_keys, list_buffer, len, &list_buffer_size);
                            if (status != NVME_SUCCESS) {
                                cqe_status = status;
                            } else {
                                size_t bytes_written = nvme_kv_write_data(req, list_buffer, list_buffer_size < len ? list_buffer_size : len);
                                if (bytes_written != list_buffer_size) {
                                   // no error is returned if there is not enough room in buffer
                                }
                                cqe_result = num_keys;
                            }
                            g_free(list_buffer);
                        }
                    }
                }
                break;
            case KV_TASK_SEND_SELECT:
                {
                    if (result->status != 0) {
                        cqe_status = NVME_KV_ERROR;
                    } else {
                        uint32_t results_id = select_results_store((unsigned char *) result->result, result->result_length);
                        cqe_result = results_id;
                        result->result = NULL;
                    }
                }
                break;
        }
        if (cqe_status != NVME_SUCCESS) {
            cqe_status |= NVME_DNR;
        }
        req->status = cqe_status;
        req->cqe.result = cpu_to_le32(cqe_result);
        nvme_enqueue_req_completion(nvme_cq(req), req);
        kv_tasks_free_result(result);
    }
}

uint16_t nvme_kv_process(NvmeCtrl *n, NvmeRequest *req) {
    switch (req->cmd.opcode) {      
    case NVME_CMD_KV_LIST:
         return nvme_kv_list(n, req);
    case NVME_CMD_KV_EXIST:
         return nvme_kv_exist(n, req);
    case NVME_CMD_KV_STORE:
         return nvme_kv_store(n, req);
    case NVME_CMD_KV_RETRIEVE:
         return nvme_kv_retrieve(n, req);
    case NVME_CMD_KV_SEND_SELECT:
         return nvme_kv_send_select(n, req);
    case NVME_CMD_KV_RETRIEVE_SELECT:
         return nvme_kv_retrieve_select(n, req);
    case NVME_CMD_KV_DELETE:
         return nvme_kv_delete(n, req);
    default:
         assert(false);
    }
}

