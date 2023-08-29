/*
 * KV Storage Functions
 *
 * Copyright (C) 2023 AirMettle, Inc.
 *
 * Written by Xinlong Yin <xinlong.yin@airmettle.com>
 *
 * This code is licensed under the GNU GPL v2 or later.
 */ 

#include "qemu/kv_store.h"
#include "qemu/query.h"
#include <pthread.h>
#include <glib/gstdio.h>

static void test_string(void) {
    kv_store_init();
    unsigned char key[4] = "key";
    unsigned char value[12] = "value\nvalue";
    g_assert(store_object(4294967295, 4294967295, key, sizeof(key), value, sizeof(value), false, false, true) ==
           sizeof(value));
    unsigned char buffer[12];
    size_t total_object_size;
    g_assert(read_object(4294967295, 4294967295, key, sizeof(key), 0, buffer, 12,
                       &total_object_size) == sizeof(value));
    g_assert(total_object_size == sizeof(value));
    g_assert(!strcmp((const char *)buffer, (const char *)value));
    g_assert(read_object(4294967295, 4294967295, key, sizeof(key), 6, buffer, 12,
                       &total_object_size) == 6);
    g_assert(total_object_size == sizeof(value));
    g_assert(!strcmp((const char *)buffer, "value"));

    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Gray", sizeof("Gray"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Bob", sizeof("Bob"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "David", sizeof("David"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Alice", sizeof("Alice"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Edmond", sizeof("Edmond"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Fred", sizeof("Fred"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    g_assert(store_object(4294967295, 4294967295, (unsigned char *) "Connor", sizeof("Connor"), value, sizeof(value),
                        false, false, true) == sizeof(value));
    size_t num_objects;
    ObjectKey* list;
    g_assert(!list_objects(4294967295, 4294967295, (unsigned char *) "David", sizeof("David"), 0, 10,
                                   &num_objects, &list));
    g_assert(num_objects == 5);
    char expected_keys[5][10] = {
            "David",
            "Edmond",
            "Fred",
            "Gray",
            "key"
    };

    for (int i = 0; i < num_objects; ++i) {
        g_assert(!strcmp((const char *)list[i].key, (const char *)expected_keys[i]));
    }
    free(list);

    g_assert(!list_objects(4294967295, 4294967295, (unsigned char *) "David", sizeof("David"), 2, 2,
                        &num_objects, &list));
    g_assert(num_objects == 2);
    for (int i = 0; i < num_objects; ++i) {
        g_assert(!strcmp((const char *)list[i].key, (const char *)expected_keys[i + 2]));
    }
    free(list);

    g_assert(!list_objects(4294967295, 4294967295, (unsigned char *) "zzz", sizeof("zzz"), 0, 0,
                    &num_objects, &list));
    g_assert(num_objects == 0);

    g_assert(!file_exist(0, 0, (unsigned char *) "Henry", sizeof("Henry")));

    g_assert(!delete_object(4294967295, 4294967295, key, sizeof(key)));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Alice", sizeof("Alice")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Bob", sizeof("Bob")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Connor", sizeof("Connor")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "David", sizeof("David")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Edmond", sizeof("Edmond")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Fred", sizeof("Fred")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char *) "Gray", sizeof("Gray")));
    g_assert(delete_object(4294967295, 4294967295, (unsigned char *) "zzz", sizeof("zzz")) == KV_ERROR_FILE_NOT_FOUND);
}

static void test_binary(void) {
    setenv("KV_BASE_DIR", "/tmp/", 1);
    kv_store_init();
    unsigned char key[6] = {0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6};
    unsigned char value[12] = {0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB,
                               0xEC};
    g_assert(store_object(4294967295, 4294967295, key, sizeof(key), value, sizeof(value), false, false, true) ==
           sizeof(value));
    unsigned char buffer[12];
    size_t total_object_size;
    g_assert(read_object(4294967295, 4294967295, key, sizeof(key), 0, buffer, 12,
                       &total_object_size) == sizeof(value));
    g_assert(total_object_size == sizeof(value));
    for (int i = 0; i < sizeof(value); ++i) {
        g_assert(buffer[i] == value[i]);
    }
    g_assert(read_object(4294967295, 4294967295, key, sizeof(key), 6, buffer, 12,
                       &total_object_size) == 6);
    g_assert(total_object_size == sizeof(value));
    for (int i = 0; i < 6; ++i) {
        g_assert(buffer[i] == value[i + 6]);
    }

    unsigned char append_value[3] = {0xED, 0xEE, 0xEF};
    g_assert(store_object(4294967295, 4294967295, key, sizeof(key), append_value,
                        sizeof(append_value), true, true, false) == sizeof(append_value));
    g_assert(read_object(4294967295, 4294967295, key, sizeof(key), 2, buffer, 12,
                       &total_object_size) == 12);
    g_assert(total_object_size == 15);
    for (int i = 2; i < sizeof(value); ++i) {
        g_assert(buffer[i - 2] == value[i]);
    }
    for (int i = 0; i < 2; ++i) {
        g_assert(buffer[i + 10] == append_value[i]);
    }

    unsigned char expected_keys[4][6] = {
            {0xE1, 0xE2, 0xE4, 0xE4, 0xE5, 0xE6},
            {0xE1, 0xE3, 0xE3, 0xE4, 0xE5, 0xE6},
            {0xE1, 0xE4, 0xE3, 0xE4, 0xE5, 0xE6},
            {0xE2, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6},
    };
    for (int i = 0; i < 4; ++i) {
        g_assert(store_object(4294967295, 4294967295, expected_keys[i], sizeof(expected_keys[i]),
                            value, sizeof(value),
                            false, false, true) == sizeof(value));
    }
    size_t num_objects;
    ObjectKey *list;
    g_assert(!list_objects(4294967295, 4294967295, expected_keys[0],
                                   sizeof(expected_keys[0]), 0, 10,
                                   &num_objects, &list));
    g_assert(num_objects == 4);

    for (int i = 0; i < num_objects; ++i) {
        for (int j = 0; j < list[i].key_len; ++j) {
            g_assert(list[i].key[j] == expected_keys[i][j]);
        }
    }
    free(list);

    g_assert(!list_objects(4294967295, 4294967295, expected_keys[0], sizeof(expected_keys[0]), 2, 10,
                        &num_objects, &list));
    g_assert(num_objects == 2);
    for (int i = 0; i < num_objects; ++i) {
        for (int j = 0; j < list[i].key_len; ++j) {
            g_assert(list[i].key[j] == expected_keys[i + 2][j]);
        }
    }
    free(list);

    g_assert(!list_objects(4294967295, 4294967295, NULL, 0, 0, 0, &num_objects, &list));
    g_assert(num_objects == 5);
    for (int j = 0; j < list[0].key_len; ++j) {
        g_assert(list[0].key[j] == key[j]);
    }
    for (int i = 1; i < num_objects; ++i) {
        for (int j = 0; j < list[i].key_len; ++j) {
            g_assert(list[i].key[j] == expected_keys[i - 1][j]);
        }
    }
    free(list);

    g_assert(!delete_object(4294967295, 4294967295, key, sizeof(key)));
    for (int i = 0; i < 4; ++i) {
        g_assert(!delete_object(4294967295, 4294967295, expected_keys[i], sizeof(expected_keys[i])));
    }
}

static void* test_json_to_csv_with_header(void* arg) {    
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"),
                      (char *)"select * from s3object",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, true, true, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "name,age,hobby,status\n"
                            "Bob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"\n"));
    free(results);
    return NULL;
}

static void* test_json_to_csv_no_header(void* arg) {   
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"), (char *)"select * from s3object",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "Bob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"\n"));
    free(results);
    return NULL;
}

static void* test_json_to_parquet(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"),
                      (char *)"select * from s3object",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_PARQUET, false, false, &results));
    free(results);
    return NULL;
}

static void* test_csv_to_json_with_header(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv"), (char *)"select * from s3object",
                      &output_len, QUERY_TYPE_CSV, QUERY_TYPE_JSON, true, true, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "{\"name\":\"Bob\",\"age\":18,\"hobby\":\"[hiking, skiing]\",\"status\":\"{'job': student, 'city': Seattle}\"}\n"));
    free(results);
    return NULL;
}

static void* test_csv_to_json_no_header(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv"), (char *)"select * from s3object",
                      &output_len, QUERY_TYPE_CSV, QUERY_TYPE_JSON, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "{\"column0\":\"Bob\",\"column1\":18,\"column2\":\"[hiking, skiing]\",\"column3\":\"{'job': student, 'city': Seattle}\"}\n"));
    free(results);
    return NULL;
}

static void* test_csv_to_csv_with_header(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv"), (char *)"select name,age from s3object",
                      &output_len, QUERY_TYPE_CSV, QUERY_TYPE_CSV, true, true, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "name,age\nBob,18\n"));
    free(results);
    return NULL;
}

static void* test_csv_to_csv_no_header(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv"), (char *)"select column0,column1 from s3object",
                      &output_len, QUERY_TYPE_CSV, QUERY_TYPE_CSV, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "Bob,18\n"));
    free(results);
    return NULL;
}

static void* test_json_to_json(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"), (char *)"select hobby,status.city from s3object",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_JSON, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "{\"hobby\":[\"hiking\",\"skiing\"],\"city\":\"Seattle\"}\n"));
    free(results);
    return NULL;
}

static void* test_json_where_clause(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (char *)"select id from s3object where userId=1",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "1\n2\n4\n"));
    free(results);
    return NULL;
}

static void* test_json_groupby_clause(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (char *)"select avg(id) from s3object group by userId",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, false, false, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "2.3333333333333335\n"
                                          "\n"
                                          "5.0\n"));
    free(results);
    return NULL;
}

static void* test_json_limit_clause(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (char *)"select * from s3object limit 1",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, false, true, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "userId,id,title,body,money\n"
                                          "1,1,sunt aut facere repellat provident occaecati excepturi optio reprehenderit,\"quia et suscipit\n"
                                          "suscipit recusandae consequuntur expedita et cum\n"
                                          "reprehenderit molestiae ut ut quas totam\n"
                                          "nostrum rerum est autem sunt rem eveniet architecto\",4.32\n"));
    free(results);
    return NULL;
}

static void* test_json_with_semicolon(void* arg) {
    size_t output_len;
    unsigned char *results;
    g_assert(!run_query(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (char *)"select * from s3object limit 1;",
                      &output_len, QUERY_TYPE_JSON, QUERY_TYPE_CSV, false, true, &results));
    char buffer[output_len + 1];
    strncpy(buffer, (char *)results, output_len);
    buffer[output_len] = '\0';
    g_assert(!strcmp((const char *)buffer, "userId,id,title,body,money\n"
                                          "1,1,sunt aut facere repellat provident occaecati excepturi optio reprehenderit,\"quia et suscipit\n"
                                          "suscipit recusandae consequuntur expedita et cum\n"
                                          "reprehenderit molestiae ut ut quas totam\n"
                                          "nostrum rerum est autem sunt rem eveniet architecto\",4.32\n"));
    free(results);
    return NULL;
}

static void test_serial(void) {
    setenv("KV_BASE_DIR", "/tmp", 1);
    kv_store_init();
    const char *json = "{\"name\":\"Bob\",\"age\":18,\"hobby\":[\"hiking\", \"skiing\"],\"status\":{\"job\": \"student\", \"city\": \"Seattle\"}}";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"), (unsigned char*)json,
                        strlen(json), false, false, false) == (strlen(json)));

    const char *json2 = "[\n"
                    "    {\"userId\": 1,\"id\": 1,\"title\": \"sunt aut facere repellat provident occaecati excepturi optio reprehenderit\",\"body\": \"quia et suscipit\\nsuscipit recusandae consequuntur expedita et cum\\nreprehenderit molestiae ut ut quas totam\\nnostrum rerum est autem sunt rem eveniet architecto\", \"money\": 4.32},\n"
                    "    {\"userId\": 1,\"id\": 2,\"title\": \"null\",\"money\": 10.32, \"body\": \"est rerum tempore vitae\\nsequi sint nihil reprehenderit dolor beatae ea dolores neque\\nfugiat blanditiis voluptate porro vel nihil molestiae ut reiciendis\\nqui aperiam non debitis possimus qui neque nisi nulla\"},\n"
                    "    {\"title\": \"null\", \"body\": \"et iusto sed quo iure\\nvoluptatem occaecati omnis eligendi aut ad\\nvoluptatem doloribus vel accusantium quis pariatur\\nmolestiae porro eius odio et labore et velit aut\"},\n"
                    "    {\"userId\": 1, \"id\": 4, \"title\": \"eum et est occaecati\", \"body\": \"abcd\", \"money\": 5.00},\n"
                    "    {\"userId\": 4, \"id\": 5, \"title\": \"nesciunt quas odio\", \"body\": \"repudiandae veniam quaerat sunt sed\\nalias aut fugiat sit autem sed est\\nvoluptatem omnis possimus esse voluptatibus quis\\nest aut tenetur dolor neque\"}\n"
                    "]";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (unsigned char*)json2,
                        strlen(json2), false, false, false) == (strlen(json2)));

    const char *csv = "Bob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv"), (unsigned char*)csv,
                        strlen(csv), false, false, false) == (strlen(csv)));

    const char *csv_with_header = "name,age,hobby,status\nBob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv"), (unsigned char*)csv_with_header,
                        strlen(csv_with_header), false, false, false) == (strlen(csv_with_header)));

    query_init_db(1);
    test_csv_to_csv_no_header(NULL);
    test_csv_to_csv_with_header(NULL);
    test_csv_to_json_no_header(NULL);
    test_csv_to_json_with_header(NULL);
    test_json_to_csv_no_header(NULL);
    test_json_to_csv_with_header(NULL);
    test_json_to_json(NULL);
    test_json_to_parquet(NULL);
    test_json_where_clause(NULL);
    test_json_groupby_clause(NULL);
    test_json_limit_clause(NULL);
    test_json_with_semicolon(NULL);

    query_close_db();
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json")));
}

static void test_concurrent(void) {
    setenv("KV_BASE_DIR", "tmp", 1);
    kv_store_init();
    const char *json = "{\"name\":\"Bob\",\"age\":18,\"hobby\":[\"hiking\", \"skiing\"],\"status\":{\"job\": \"student\", \"city\": \"Seattle\"}}";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json"), (unsigned char*)json,
                        strlen(json), false, false, false) == (strlen(json)));

    const char *json2 = "[\n"
                       "    {\"userId\": 1,\"id\": 1,\"title\": \"sunt aut facere repellat provident occaecati excepturi optio reprehenderit\",\"body\": \"quia et suscipit\\nsuscipit recusandae consequuntur expedita et cum\\nreprehenderit molestiae ut ut quas totam\\nnostrum rerum est autem sunt rem eveniet architecto\", \"money\": 4.32},\n"
                       "    {\"userId\": 1,\"id\": 2,\"title\": \"null\",\"money\": 10.32, \"body\": \"est rerum tempore vitae\\nsequi sint nihil reprehenderit dolor beatae ea dolores neque\\nfugiat blanditiis voluptate porro vel nihil molestiae ut reiciendis\\nqui aperiam non debitis possimus qui neque nisi nulla\"},\n"
                       "    {\"title\": \"null\", \"body\": \"et iusto sed quo iure\\nvoluptatem occaecati omnis eligendi aut ad\\nvoluptatem doloribus vel accusantium quis pariatur\\nmolestiae porro eius odio et labore et velit aut\"},\n"
                       "    {\"userId\": 1, \"id\": 4, \"title\": \"eum et est occaecati\", \"body\": \"abcd\", \"money\": 5.00},\n"
                       "    {\"userId\": 4, \"id\": 5, \"title\": \"nesciunt quas odio\", \"body\": \"repudiandae veniam quaerat sunt sed\\nalias aut fugiat sit autem sed est\\nvoluptatem omnis possimus esse voluptatibus quis\\nest aut tenetur dolor neque\"}\n"
                       "]";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json"), (unsigned char*)json2,
                        strlen(json2), false, false, false) == (strlen(json2)));

    const char *csv = "Bob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv"), (unsigned char*)csv,
                        strlen(csv), false, false, false) == (strlen(csv)));

    const char *csv_with_header = "name,age,hobby,status\nBob,18,\"[hiking, skiing]\",\"{'job': student, 'city': Seattle}\"";
    g_assert(store_object(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv"), (unsigned char*)csv_with_header,
                        strlen(csv_with_header), false, false, false) == (strlen(csv_with_header)));

    query_init_db(3);
    pthread_t threads[12];
    pthread_create(&threads[0], NULL, test_csv_to_csv_no_header, NULL);
    pthread_create(&threads[1], NULL, test_csv_to_csv_with_header, NULL);
    pthread_create(&threads[2], NULL, test_csv_to_json_no_header, NULL);
    pthread_create(&threads[3], NULL, test_csv_to_json_with_header, NULL);
    pthread_create(&threads[4], NULL, test_json_to_csv_no_header, NULL);
    pthread_create(&threads[5], NULL, test_json_to_csv_with_header, NULL);
    pthread_create(&threads[6], NULL, test_json_to_json, NULL);
    pthread_create(&threads[7], NULL, test_json_to_parquet, NULL);
    pthread_create(&threads[8], NULL, test_json_where_clause, NULL);
    pthread_create(&threads[9], NULL, test_json_groupby_clause, NULL);
    pthread_create(&threads[10], NULL, test_json_limit_clause, NULL);
    pthread_create(&threads[11], NULL, test_json_with_semicolon, NULL);
    for (int i = 0; i < 12; ++i) {
        pthread_join(threads[i], NULL);
    }
    query_close_db();
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test.json", sizeof("test.json")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test.csv", sizeof("test.csv")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test_with_header.csv", sizeof("test_with_header.csv")));
    g_assert(!delete_object(4294967295, 4294967295, (unsigned char*)"test2.json", sizeof("test2.json")));
}

int main(int argc, char **argv)
{
    g_test_init(&argc, &argv, NULL);
    g_test_add_func("/kv/test_string", test_string);
    g_test_add_func("/kv/test_binary", test_binary);
    g_test_add_func("/kv/test_serial", test_serial);
    g_test_add_func("/kv/test_concurrent", test_concurrent);
    return g_test_run();
}
