#include "src/kv_single/client.h"
#include "lib/cjson/cJSON.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

struct kv_single_client {
    rpc_client_t *endpoint;
};

kv_single_client_t *kv_single_client_new(rpc_client_t *endpoint) {
    kv_single_client_t *c = calloc(1, sizeof(kv_single_client_t));
    c->endpoint = endpoint;
    return c;
}

void kv_single_client_free(kv_single_client_t *c) {
    if (c) {
        rpc_client_free(c->endpoint);
        free(c);
    }
}

static void client_get(void *ctx, const char *key,
                       char **out_value, version_t *out_version, kv_err_t *out_err) {
    kv_single_client_t *c = (kv_single_client_t *)ctx;
    (void)c;

    /* DONE: call get and decode the reply. */

    //building da jason (gta 6) reqs
    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "key", key);
    char* content = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);
    char* reply = NULL;

    while(1) {
        reply = rpc_call(c->endpoint, "get", content);
        if (reply != NULL) {break;}
        usleep(10000);
    }

    //decryption/decoding json resp
    cJSON *resp = cJSON_Parse(reply);
    cJSON *node_val = cJSON_GetObjectItemCaseSensitive(resp, "value");
    cJSON *node_ver = cJSON_GetObjectItemCaseSensitive(resp, "version");
    cJSON *node_err = cJSON_GetObjectItemCaseSensitive(resp, "err");

    //parsing
    *out_value = (node_val && cJSON_IsString(node_val) ? strdup(node_val->valuestring): strdup(""));
    *out_version = (node_ver && cJSON_IsNumber(node_ver)) ? (version_t)node_ver->valuedouble: 0;
    *out_err = node_err ? kv_err_from_str(node_err->valuestring): KV_NO_KEY;

    cJSON_Delete(resp);
    free(content);
    free(reply);
                        
}

static kv_err_t client_put(void *ctx, const char *key, const char *value, version_t ver) {
    kv_single_client_t *c = (kv_single_client_t *)ctx;
    (void)c;

    /* DONE: call put and decode the reply. */

    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "key", key);
    cJSON_AddStringToObject(req, "value", value);
    cJSON_AddNumberToObject(req, "version", (double)ver);
    char* content = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);
    char* reply = NULL;

    //attempt num
    bool attempt_num_one = true;
    kv_err_t err_f = KV_OK;

    while(1) {
        reply = rpc_call(c->endpoint, "put", content);
        if (reply != NULL) {break;}
        attempt_num_one = false;
        usleep(10000);
    }

    //decryption/decoding json resp
    cJSON *resp = cJSON_Parse(reply);
    cJSON *node_err = cJSON_GetObjectItemCaseSensitive(resp, "err");

    err_f = node_err ? kv_err_from_str(node_err->valuestring) : KV_NO_KEY;
    if (!attempt_num_one && err_f == KV_VERSION) {
        err_f = KV_MAYBE;
    }

    cJSON_Delete(resp);
    free(content);
    free(reply);

    return err_f;
}

kv_client_t kv_single_client_as_kv_client(kv_single_client_t *c) {
    kv_client_t out;
    out.ctx = c;
    out.get = client_get;
    out.put = client_put;
    return out;
}
