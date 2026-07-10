#include "src/lock.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

lock_t *lock_new(kv_client_t *client, const char *lockname) {
    lock_t *l = calloc(1, sizeof(lock_t));
    l->client = client;
    // DONE: initialize lock identity and version.
    l->l_name = strdup(lockname);
    l->h_id = calloc(1, 64);
    if (l->h_id) {
        snprintf(l->h_id, 64, "client_%d_%p", getpid(), (void*)l);
    }

    return l;
}

void lock_free(lock_t *l) {
    if (l) {
        free(l->l_name);
        free(l->h_id);
        free(l);
    }
}

void lock_acquire(lock_t *l) {
    // DONE: acquire the lock through the KV client.
    while (1) {
        char* val = NULL;
        version_t ver = 0;
        kv_err_t err;
        l->client->get(l->client->ctx, l->l_name, &val, &ver, &err);
        if (err == KV_NO_KEY || (err == KV_OK && val != NULL && strcmp(val, "") == 0)) {
            kv_err_t put = l->client->put(l->client->ctx, l->l_name, l->h_id, ver);
            if (put == KV_OK) {
                if (val) free(val);
                return;
            }
        }
        else if (err == KV_OK && val != NULL && strcmp(val, l->h_id) == 0) {
            if (val) free(val);
            return;
        }
        free(val);
        usleep(10000);
    }
}

void lock_release(lock_t *l) {
    // DONE: release the lock if this client owns it.
    while (1) {
        char* val = NULL;
        version_t ver = 0;
        kv_err_t err;
        l->client->get(l->client->ctx, l->l_name, &val, &ver, &err);
        if (err == KV_OK && val != NULL && strcmp(val, l->h_id) ==0) {
            kv_err_t put = l->client->put(l->client->ctx, l->l_name, "", ver);
            if (put == KV_OK) {
                free(val);
                return;
            }
        }
        else {
            if(val) free(val);
            return;
        }
        if (val) free(val);
        usleep(10000);
    }
}
