#include "src/kv_raft/client.h"
#include "lib/cjson/cJSON.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdbool.h>

struct kvraft_client {
    rpc_client_t **servers;
    int n_servers;
    // DONE: cache leader and client state.
    atomic_int leader_idx; //cuz of version puts
};

kvraft_client_t *kvraft_client_new(rpc_client_t **servers, int n_servers) {
    kvraft_client_t *c = calloc(1, sizeof(kvraft_client_t));
    c->servers = malloc(n_servers * sizeof(rpc_client_t *));
    memcpy(c->servers, servers, n_servers * sizeof(rpc_client_t *));
    c->n_servers = n_servers;
    return c;
}

void kvraft_client_free(kvraft_client_t *c) {
    if (!c) return;
    free(c->servers);
    free(c);
}

static void client_get(void *ctx, const char *key,
                       char **out_value, version_t *out_version, kv_err_t *out_err) {
    kvraft_client_t *c = (kvraft_client_t *)ctx;
    (void)c;
    // DONE: try servers until a get succeeds.
    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "key", key);
    char *content = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);
    while (1) {
        int leader = atomic_load(&c->leader_idx);
        char *reply = rpc_call_timeout(c->servers[leader], "get", content, 1);
        if (!reply) {
            atomic_store(&c->leader_idx, (leader+1) % c->n_servers);
            continue;
        }
        cJSON *resp = cJSON_Parse(reply);
        free(reply);
        if(!resp) {
            atomic_store(&c->leader_idx, (leader+1) %c->n_servers);
            continue;
        }
        char *err = cJSON_GetObjectItem(resp, "err")->valuestring;
        if (strcmp(err, "WrongLeader")==0) {
            atomic_store(&c->leader_idx, (leader+1)%c->n_servers);
            cJSON_Delete(resp);
            continue;
        }
        *out_value = strdup(cJSON_GetObjectItem(resp, "value")->valuestring);
        *out_version = (version_t)cJSON_GetObjectItem(resp, "version")->valuedouble;
        if (strcmp(err, "OK") ==0) *out_err = KV_OK;
        else if (strcmp(err, "NoKey")==0) *out_err = KV_NO_KEY;
        else if (strcmp(err, "Version")==0) *out_err = KV_VERSION;
        else if (strcmp(err, "Maybe")==0) *out_err = KV_MAYBE;
        cJSON_Delete(resp);
        break;
    }
    free(content);
}

static kv_err_t client_put(void *ctx, const char *key, const char *value, version_t ver) {
    kvraft_client_t *c = (kvraft_client_t *)ctx;
    (void)c;
    // DONE: retry puts and report Maybe after uncertain failures.
    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "key", key);
    cJSON_AddStringToObject(req, "value", value);
    cJSON_AddNumberToObject(req, "version", (double)ver);
    char *content = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);
    bool re_attempt_flag = false; //retry poss?
    while (1) {
        int leader = atomic_load(&c->leader_idx);
        char *reply = rpc_call_timeout(c->servers[leader], "put", content, 1);
        if (!reply) {
            re_attempt_flag = true;
            atomic_store(&c->leader_idx, (leader+1) % c->n_servers);
            continue;
        }
        cJSON *resp = cJSON_Parse(reply);
        free(reply);
        if(!resp) {
            re_attempt_flag = true;
            atomic_store(&c->leader_idx, (leader+1) %c->n_servers);
            continue;
        }
        char *err = cJSON_GetObjectItem(resp, "err")->valuestring;
        if (strcmp(err, "WrongLeader")==0) {
            atomic_store(&c->leader_idx, (leader+1)%c->n_servers);
            cJSON_Delete(resp);
            continue;
        }
        kv_err_t out_err = KV_OK;
        if (strcmp(err, "OK") ==0) out_err = KV_OK;
        else if (strcmp(err, "NoKey")==0) out_err = KV_NO_KEY;
        else if (strcmp(err, "Version")==0) out_err = KV_VERSION;
        else if (strcmp(err, "Maybe")==0) out_err = KV_MAYBE;
        cJSON_Delete(resp);
        if (out_err == KV_VERSION && re_attempt_flag) {
            free(content);
            return KV_MAYBE;
        }
        free(content);
        return out_err;
    }
}

kv_client_t kvraft_client_as_kv_client(kvraft_client_t *c) {
    kv_client_t out;
    out.ctx = c;
    out.get = client_get;
    out.put = client_put;
    return out;
}