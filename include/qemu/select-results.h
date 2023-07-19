/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Chia-Lin Wu <cwu@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#ifndef SELECT_RESULTS_H
#define SELECT_RESULTS_H

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

void select_results_init(void);
uint32_t select_results_store(unsigned char *results, size_t results_len);
unsigned char *select_results_retrieve(uint32_t id, size_t *data_len, bool do_not_remove,
                                       bool do_not_remove_if_size_gt, size_t size_check, bool *found);

#endif
