#include "src/kv_single/server.h"
#include "lib/hash.h"
#include "lib/kv_types.h"
#include "lib/cjson/cJSON.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>

typedef struct { char *key; } get_args_t;
typedef struct { char *key; char *value; version_t version; } put_args_t;

static bool parse_get_args(cJSON *j, get_args_t *out) {
    cJSON *k = cJSON_GetObjectItemCaseSensitive(j, "key");
    if (!cJSON_IsString(k)) return false;
    out->key = k->valuestring;
    return true;
}

static bool parse_put_args(cJSON *j, put_args_t *out) {
    cJSON *k = cJSON_GetObjectItemCaseSensitive(j, "key");
    cJSON *v = cJSON_GetObjectItemCaseSensitive(j, "value");
    cJSON *ver = cJSON_GetObjectItemCaseSensitive(j, "version");
    if (!cJSON_IsString(k) || !cJSON_IsString(v) || !cJSON_IsNumber(ver)) return false;
    out->key = k->valuestring;
    out->value = v->valuestring;
    out->version = (version_t)ver->valuedouble;
    return true;
}

static char *make_get_reply(const char *value, version_t version, kv_err_t err) {
    cJSON *j = cJSON_CreateObject();
    cJSON_AddStringToObject(j, "value", value ? value : "");
    cJSON_AddNumberToObject(j, "version", (double)version);
    cJSON_AddStringToObject(j, "err", kv_err_to_str(err));
    char *s = cJSON_PrintUnformatted(j);
    cJSON_Delete(j);
    return s;
}

static char *make_put_reply(kv_err_t err) {
    cJSON *j = cJSON_CreateObject();
    cJSON_AddStringToObject(j, "err", kv_err_to_str(err));
    char *s = cJSON_PrintUnformatted(j);
    cJSON_Delete(j);
    return s;
}

/* DONE: define the stored key/value entries. */
typedef struct {
    char *key;
    char *val;
    version_t ver;
    struct hash_elem elem;
} kv_entry_t;

//hashing fns
static unsigned kv_hash(const struct hash_elem *e, void *aux) {
    kv_entry_t *entry = hash_entry(e, kv_entry_t, elem);
    return hash_bytes(entry->key, strlen(entry->key));
}

//comparison
static bool kv_less(const struct hash_elem *a, const struct hash_elem *b, void *aux) {
    kv_entry_t *e_a = hash_entry(a, kv_entry_t, elem);
    kv_entry_t *e_b = hash_entry(b, kv_entry_t, elem);
    return strcmp(e_a->key, e_b->key) < 0;
}

//destructor
static void kv_free_entry(struct hash_elem *e, void *aux) {
    kv_entry_t *entry = hash_entry(e, kv_entry_t, elem);
    free(entry->key);
    free(entry->val);
    free(entry);
}

struct kv_server {
    /* DONE: add hash table and lock fields. */
    struct hash table;
    pthread_mutex_t lock;
};

kv_server_t *kv_server_new(void) {
    /* DONE: initialize server state. */
    kv_server_t *s = calloc(1, sizeof(kv_server_t));
    pthread_mutex_init(&s->lock, NULL);
    hash_init(&s->table, kv_hash, kv_less, NULL);
    return s;
}

void kv_server_free(kv_server_t *s) {
    /* DONE: free server state. */

    if (!s) return;
    pthread_mutex_lock(&s->lock);
    hash_destroy(&s->table, kv_free_entry);
    pthread_mutex_unlock(&s->lock);
    pthread_mutex_destroy(&s->lock);
    free(s);
}

static char *handle_get(kv_server_t *s, cJSON *args) {
    get_args_t a = {0};
    if (!parse_get_args(args, &a))
        return make_get_reply("", 0, KV_NO_KEY);

    /* DONE: return the value and version for a.key. */
    pthread_mutex_lock(&s->lock);
    kv_entry_t entry_search;
    entry_search.key = a.key;
    struct hash_elem *e = hash_find(&s->table, &entry_search.elem);
    char* reply;
    if (e) {
        kv_entry_t *entry = hash_entry(e, kv_entry_t, elem);
        reply = make_get_reply(entry->val, entry->ver, KV_OK);
    } else {
        reply = make_get_reply("", 0, KV_NO_KEY);
    }
    pthread_mutex_unlock(&s->lock);

    return reply;
}

static char *handle_put(kv_server_t *s, cJSON *args) {
    put_args_t a = {0};
    if (!parse_put_args(args, &a))
        return make_put_reply(KV_NO_KEY);

    /* DONE: apply versioned put semantics. */
    pthread_mutex_lock(&s->lock);
    kv_entry_t entry_search;
    entry_search.key = a.key;
    struct hash_elem *e = hash_find(&s->table, &entry_search.elem);
    char *reply;
    if (e) {
        kv_entry_t *entry = hash_entry(e, kv_entry_t, elem);
        if (entry->ver != a.version) {
            reply = make_put_reply(KV_VERSION);
        }
        else {
            free(entry->val);
            entry->val = strdup(a.value);
            entry->ver++;
            reply = make_put_reply(KV_OK);
        }
    } else {
        if (a.version != 0) {
            reply = make_put_reply(KV_NO_KEY);
        } else {
            kv_entry_t *ne = malloc(sizeof(kv_entry_t));
            ne->key = strdup(a.key);
            ne->val = strdup(a.value);
            ne->ver = 1;
            hash_insert(&s->table, &ne->elem);
            reply = make_put_reply(KV_OK);
        }
    }
    pthread_mutex_unlock(&s->lock);

    return reply;
}

char *kv_server_dispatch(void *ctx, const char *method, const char *body, int client_id) {
    kv_server_t *s = (kv_server_t *)ctx;
    (void)client_id;

    cJSON *args = cJSON_Parse(body);
    if (!args) return strdup("{\"err\":\"ParseError\"}");

    char *result = NULL;
    if (strcmp(method, "get") == 0) {
        result = handle_get(s, args);
    } else if (strcmp(method, "put") == 0) {
        result = handle_put(s, args);
    } else {
        result = strdup("{\"err\":\"UnknownMethod\"}");
    }

    cJSON_Delete(args);
    return result;
}
