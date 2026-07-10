#include "src/kv_raft/raft.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include "lib/cjson/cJSON.h" 

/* DONE: define RPC payloads and log entries. */

typedef struct {
    raft_t *r;
    int peer_idx;
    int args_term;
    int args_last_log_term;
    int args_last_log_idx;
} rv_args_t;

typedef struct {
    raft_t *r;
    char *content;
    int peer_idx;
    int args_term;
    int num_entr_sent;
    int prev_log_idx;
} ae_args_t;

static void lock_persisted(raft_t *r);

static long get_time_in_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void *send_req_vote(void *arg) {
    rv_args_t *args = (rv_args_t*) arg;
    raft_t* r = args->r;
    cJSON *req = cJSON_CreateObject();
    cJSON_AddNumberToObject(req, "term", (double)args->args_term);
    cJSON_AddNumberToObject(req, "candidateId", (double)r->me);
    cJSON_AddNumberToObject(req, "lastLogTerm", (double)args->args_last_log_term);
    cJSON_AddNumberToObject(req, "lastLogIndex", (double)args->args_last_log_idx);
    char* content = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);
    char* reply = rpc_call_timeout(r->peers[args->peer_idx], "request_vote", content, 1);
    free(content);
    if (reply) {
        cJSON *resp = cJSON_Parse(reply);
        if (resp) {
            int reply_term = cJSON_GetObjectItem(resp, "term")->valueint;
            bool vote_granted = cJSON_GetObjectItem(resp, "voteGranted")->valueint;
            pthread_mutex_lock(&r->lock);
            if (reply_term > r->curTerm) {
                r->curTerm = reply_term;
                r->state = FOLLOWER;
                r->voted4 = -1;
                long cur = get_time_in_ms();
                r->last_heard_from_leader.tv_sec = cur/1000;
                r->last_heard_from_leader.tv_nsec = (cur%1000)*1000000;
                lock_persisted(r);
            }
            else if (r->state == CANDIDATE && r->curTerm == args->args_term && vote_granted) {
                r->votes_received++;
                if (r->n_peers / 2 < r->votes_received) {
                    r->state = LEADER;
                    for (int i = 0; i < r->n_peers; ++i) {
                        r->nextIdx[i] = r->logLen;
                        r->matchIdx[i] = 0;
                        r->in_flight[i] = false;
                    }
                }
            }
            pthread_mutex_unlock(&r->lock);
            cJSON_Delete(resp);
        }
        free(reply);
    }
    free(args);
    return NULL;
}

void *send_append_entr(void *arg) {
    ae_args_t *args = (ae_args_t*) arg;
    raft_t *r = args->r;
    char* reply = rpc_call_timeout(r->peers[args->peer_idx], "append_entries", args->content, 1);
    cJSON *resp = NULL;
    if (reply) resp = cJSON_Parse(reply);
    pthread_mutex_lock(&r->lock);
    r->in_flight[args->peer_idx] = false;
    if (resp) {
        int reply_term = cJSON_GetObjectItem(resp, "term")->valueint;
        int success = cJSON_GetObjectItem(resp, "success")->valueint;
        if (reply_term > r->curTerm) {
            r->curTerm = reply_term;
            r->state = FOLLOWER;
            r->voted4 = -1;
            long cur = get_time_in_ms();
            r->last_heard_from_leader.tv_sec = cur/1000;
            r->last_heard_from_leader.tv_nsec = (cur%1000)*1000000;
            lock_persisted(r);
        }
        else if (r->state == LEADER && args->args_term == r->curTerm) {
            if (success) {
                int new_match = args->prev_log_idx + args->num_entr_sent;
                if (new_match > r->matchIdx[args->peer_idx]) {
                    r->matchIdx[args->peer_idx] = new_match;
                    r->nextIdx[args->peer_idx] = new_match + 1;
                }
                for (int n = r->logLen-1; n > r->commitIdx; --n) {
                    if (r->log[n].term == r->curTerm) {
                        int match_cnt = 1;
                        for (int i = 0; i < r->n_peers; ++i) {
                            if (i == r->me) continue;
                            if (r->matchIdx[i] >= n) match_cnt++;
                        }
                        if (match_cnt > r->n_peers / 2) {
                            r->commitIdx = n;
                            break;
                        }
                    }
                }
            } else {
                cJSON *ct_item = cJSON_GetObjectItem(resp, "conflictTerm");
                cJSON *ci_item = cJSON_GetObjectItem(resp, "conflictIndex");
                if (ct_item && ci_item) {
                    int c_term = ct_item->valueint;
                    int c_idx = ci_item->valueint;
                    if (c_term == -1) {
                        r->nextIdx[args->peer_idx] = c_idx;
                    } else {
                        int found_idx = -1;
                        for (int i = args->prev_log_idx; i > 0; i--) {
                            if (r->log[i].term == (uint64_t)c_term) {
                                found_idx = i;
                                break;
                            }
                        }
                        if (found_idx != -1) {
                            r->nextIdx[args->peer_idx] = found_idx + 1;
                        } else {
                            r->nextIdx[args->peer_idx] = c_idx;
                        }
                    }
                } else {
                    if (r->nextIdx[args->peer_idx] > 1) r->nextIdx[args->peer_idx]--;
                }
                if (r->nextIdx[args->peer_idx] < 1) r->nextIdx[args->peer_idx] = 1;
            }
        }
        cJSON_Delete(resp);
    }
    pthread_mutex_unlock(&r->lock);
    if (reply) free(reply);
    free(args->content);
    free(args);
    return NULL;
}

void *election_timer_thread(void* arg) {
    raft_t *r = (raft_t*) arg;
    int timeout = 400 + (rand_r(&r->seed) % 401); 
    while (r->running) {
        usleep(10000); 
        pthread_mutex_lock(&r->lock);
        long cur = get_time_in_ms();
        long last = (r->last_heard_from_leader.tv_sec * 1000 + r->last_heard_from_leader.tv_nsec / 1000000);
        if (r->state != LEADER && (cur - last) > timeout) {
            r->state = CANDIDATE;
            r->curTerm++;
            r->voted4 = r->me;
            r->votes_received = 1;
            r->last_heard_from_leader.tv_sec = cur / 1000;
            r->last_heard_from_leader.tv_nsec = (cur % 1000) * 1000000;
            lock_persisted(r);   
            int last_log_idx = r->logLen -1;
            int last_log_term = r->log[last_log_idx].term;
            int cur_term = r->curTerm;
            timeout = 400 + (rand_r(&r->seed) % 401);  
            for (int i = 0; i < r->n_peers; ++i) {
                if (i == r->me) continue;
                rv_args_t *args = malloc(sizeof(rv_args_t));
                args->r = r;
                args->peer_idx = i;
                args->args_term = cur_term;
                args->args_last_log_idx = last_log_idx;
                args->args_last_log_term = last_log_term;
                pthread_t tid;
                if (pthread_create(&tid, NULL, send_req_vote, args) == 0) {
                    pthread_detach(tid);
                } else {
                    free(args);
                }
            }
        }
        pthread_mutex_unlock(&r->lock);
    }
    return NULL;
}

void *heartbeat_thread(void* arg) {
    raft_t *r = (raft_t*) arg;
    while (r->running) {
        usleep(50000);
        pthread_mutex_lock(&r->lock);
        if (r->state == LEADER) {
            for (int i = 0; i < r->n_peers; i++) {
                if (i == r->me) continue;
                if (r->in_flight[i]) continue;
                r->in_flight[i] = true;
                int prev_log_idx = r->nextIdx[i] -1;
                int prev_log_term = r->log[prev_log_idx].term;
                int num_entr_sent = 0;
                cJSON *req = cJSON_CreateObject();
                cJSON_AddNumberToObject(req, "term", (double)r->curTerm);
                cJSON_AddNumberToObject(req, "leaderId", (double)r->me);
                cJSON_AddNumberToObject(req, "prevLogIndex", (double)prev_log_idx);
                cJSON_AddNumberToObject(req, "prevLogTerm", (double)prev_log_term);
                cJSON_AddNumberToObject(req, "leaderCommit", (double)r->commitIdx);
                cJSON *entries = cJSON_CreateArray();
                for (int j = r->nextIdx[i]; j < r->logLen; j++) {
                    cJSON *entry = cJSON_CreateObject();
                    cJSON_AddNumberToObject(entry, "term", (double)r->log[j].term);
                    cJSON_AddStringToObject(entry, "cmd", r->log[j].cmd);
                    cJSON_AddItemToArray(entries, entry);
                    num_entr_sent++;  
                    if (num_entr_sent >= 200) break; 
                }
                cJSON_AddItemToObject(req, "entries", entries);
                char* content = cJSON_PrintUnformatted(req);
                cJSON_Delete(req);  
                ae_args_t *args = malloc(sizeof(ae_args_t));
                args->r = r;
                args->peer_idx = i;
                args->content = content;
                args->args_term = r->curTerm;
                args->num_entr_sent = num_entr_sent;
                args->prev_log_idx = prev_log_idx;
                pthread_t tid;
                if (pthread_create(&tid, NULL, send_append_entr, args) == 0) {
                    pthread_detach(tid);
                } else {
                    r->in_flight[i] = false;
                    free(args->content);
                    free(args);
                }
            }
        }
        pthread_mutex_unlock(&r->lock);
    }
    return NULL;
}

static void lock_persisted(raft_t *r) {
    cJSON *j = cJSON_CreateObject();
    cJSON_AddNumberToObject(j, "curTerm", (double)r->curTerm);
    cJSON_AddNumberToObject(j, "voted4", (double)r->voted4);
    cJSON *log_arr = cJSON_CreateArray();
    for (int i = 0; i < r->logLen; i++) {
        cJSON *entry = cJSON_CreateObject();
        cJSON_AddNumberToObject(entry, "term", (double)r->log[i].term);
        cJSON_AddStringToObject(entry, "cmd", r->log[i].cmd ? r->log[i].cmd : "");
        cJSON_AddItemToArray(log_arr, entry);
    }
    cJSON_AddItemToObject(j, "log", log_arr);
    char* info = cJSON_PrintUnformatted(j);
    persister_save(r->persister, info);
    free(info);
    cJSON_Delete(j);
}

static void restore(raft_t *r) {
    char* info = persister_read(r->persister);
    if (!info || strlen(info) == 0) {
        if (info) free(info);
        return;
    }
    cJSON *j = cJSON_Parse(info);
    if (j) {
        r->curTerm = cJSON_GetObjectItem(j, "curTerm")->valueint;
        r->voted4 = cJSON_GetObjectItem(j, "voted4")->valueint;
        cJSON *log_arr = cJSON_GetObjectItem(j, "log");
        int arr_size = cJSON_GetArraySize(log_arr);
        if (r->log) {
            for (int i = 0; i < r->logLen; i++) {
                if (r->log[i].cmd) free(r->log[i].cmd);
            }
            free(r->log);
        }
        r->logCap = arr_size + 100;
        r->log = calloc(r->logCap, sizeof(log_entry_t));
        r->logLen = arr_size;
        for (int i = 0; i < arr_size; ++i) {
            cJSON *entry = cJSON_GetArrayItem(log_arr, i);
            r->log[i].term = cJSON_GetObjectItem(entry, "term")->valueint;
            r->log[i].cmd = strdup(cJSON_GetObjectItem(entry, "cmd")->valuestring);
        }
        cJSON_Delete(j);
    }
    free(info);
}

raft_t *raft_new(int me, rpc_client_t **peers, int n_peers, persister_t *p) {
    raft_t *r = calloc(1, sizeof(raft_t));
    r->me = me;
    r->n_peers = n_peers;
    r->peers = peers;
    r->persister = p;
    pthread_mutex_init(&r->lock, NULL);
    r->state = FOLLOWER;
    r->voted4 = -1;
    r->running = true;
    r->seed = get_time_in_ms() ^ (getpid() << 16) ^ (me << 24) ^ (intptr_t)r;
    for (int i = 0; i < 16; i++) {
        r->in_flight[i] = false;
    }
    long cur = get_time_in_ms();
    r->last_heard_from_leader.tv_sec = cur / 1000;
    r->last_heard_from_leader.tv_nsec = (cur % 1000) * 1000000;
    r->logCap = 100;
    r->log = calloc(r->logCap, sizeof(log_entry_t));
    r->log[0].term = 0;
    r->log[0].cmd = strdup("");
    r->logLen = 1;
    r->nextIdx = calloc(n_peers, sizeof(int));
    r->matchIdx = calloc(n_peers, sizeof(int));
    restore(r);
    pthread_create(&r->tticker, NULL, election_timer_thread, r);
    pthread_create(&r->theartbeat, NULL, heartbeat_thread, r);

    return r;
}

void raft_free(raft_t *r) {
    if (!r) return;
    r->running = false;
    usleep(100000);
    for (int i = 0; i < r->logLen; i++) {
        if (r->log[i].cmd) free(r->log[i].cmd);
    }
    free(r->log);
    free(r->nextIdx);
    free(r->matchIdx);
    pthread_mutex_destroy(&r->lock);
    free(r);
}

void raft_get_state(raft_t *r, uint64_t *term, bool *is_leader) {
    pthread_mutex_lock(&r->lock);
    *term = r->curTerm;
    *is_leader = (r->state == LEADER);
    pthread_mutex_unlock(&r->lock);
}

int raft_submit(raft_t *r, const char *cmd, uint64_t *term, bool *is_leader) {
    pthread_mutex_lock(&r->lock);
    *term = r->curTerm;
    *is_leader = (r->state == LEADER);
    if (r->state != LEADER) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (r->logLen == r->logCap) {
        int oldCap = r->logCap;
        r->logCap *= 2;
        r->log = realloc(r->log, r->logCap * sizeof(log_entry_t));
        memset(r->log + oldCap, 0, (r->logCap - oldCap)*sizeof(log_entry_t));
    }
    int idx = r->logLen;
    r->log[idx].term = r->curTerm;
    r->log[idx].cmd = strdup(cmd);
    r->logLen++;
    lock_persisted(r);
    pthread_mutex_unlock(&r->lock);
    return idx;
}

char *raft_get_committed(raft_t *r, int index) {
    pthread_mutex_lock(&r->lock);
    char* cmd = NULL;
    if (index <= r->commitIdx && index < r->logLen) {
        cmd = strdup(r->log[index].cmd);
    }
    pthread_mutex_unlock(&r->lock);
    return cmd;
}

char *raft_dispatch(void *ctx, const char *method, const char *body, int client_id) {
    raft_t *r = (raft_t*)ctx;
    cJSON *args = cJSON_Parse(body);
    if (!args) return strdup("{\"error\":\"invalid json\"}");
    cJSON* reply = cJSON_CreateObject();
    if (strcmp(method, "request_vote") == 0) {
        int args_term = cJSON_GetObjectItem(args, "term")->valueint;
        int cand_id = cJSON_GetObjectItem(args, "candidateId")->valueint;
        int last_log_idx = cJSON_GetObjectItem(args, "lastLogIndex")->valueint;
        int last_log_term = cJSON_GetObjectItem(args, "lastLogTerm")->valueint;
        pthread_mutex_lock(&r->lock);
        if (args_term > r->curTerm) {
            r->curTerm = args_term;
            r->state = FOLLOWER;
            r->voted4 = -1;
            lock_persisted(r);
        }
        int my_last_log_idx = r->logLen -1;
        int my_last_log_term = r->log[my_last_log_idx].term;
        int log_good = (last_log_term > my_last_log_term) || (last_log_term == my_last_log_term && last_log_idx >= my_last_log_idx);
        int vote_granted = 0;
        if (args_term == r->curTerm && (r->voted4 == -1 || r->voted4 == cand_id) && log_good) {
            r->voted4 = cand_id;
            vote_granted = 1;
            long cur = get_time_in_ms();
            r->last_heard_from_leader.tv_sec = cur / 1000;
            r->last_heard_from_leader.tv_nsec = (cur % 1000) * 1000000;
            lock_persisted(r);
        }
        cJSON_AddNumberToObject(reply, "term", (double)r->curTerm);
        cJSON_AddNumberToObject(reply, "voteGranted", vote_granted);
        pthread_mutex_unlock(&r->lock);
    }
    else if (strcmp(method, "append_entries") == 0) {
        int args_term = cJSON_GetObjectItem(args, "term")->valueint;
        int prev_log_idx = cJSON_GetObjectItem(args, "prevLogIndex")->valueint;
        int prev_log_term = cJSON_GetObjectItem(args, "prevLogTerm")->valueint;
        int leader_commit = cJSON_GetObjectItem(args, "leaderCommit")->valueint;
        cJSON *entries = cJSON_GetObjectItem(args, "entries");
        pthread_mutex_lock(&r->lock);
        int success = 0;
        int conflict_term = -1;
        int conflict_idx = -1;
        if (args_term >= r->curTerm) {
            if (args_term > r->curTerm) {
                r->curTerm = args_term;
                r->voted4 = -1;
                lock_persisted(r);
            }
            r->state = FOLLOWER;
            long cur = get_time_in_ms();
            r->last_heard_from_leader.tv_sec = cur /1000;
            r->last_heard_from_leader.tv_nsec = (cur % 1000) * 1000000;   
            if (prev_log_idx >= r->logLen) {
                conflict_idx = r->logLen;
                conflict_term = -1;
            } else if (r->log[prev_log_idx].term != prev_log_term) {
                conflict_term = r->log[prev_log_idx].term;
                conflict_idx = prev_log_idx;
                while (conflict_idx > 1 && r->log[conflict_idx - 1].term == conflict_term) {
                    conflict_idx--;
                }
            } else {
                success = 1;
                int num_entr = cJSON_GetArraySize(entries);
                for (int i = 0; i < num_entr; ++i) {
                    cJSON *entr = cJSON_GetArrayItem(entries, i);
                    int e_term = cJSON_GetObjectItem(entr, "term")->valueint;
                    char *e_cmd = cJSON_GetObjectItem(entr, "cmd")->valuestring;
                    int log_idx = prev_log_idx + i + 1;
                    if (log_idx < r->logLen) {
                        if (r->log[log_idx].term != e_term) {
                            for (int j = log_idx; j < r->logLen; j++) {
                                free(r->log[j].cmd);
                            }
                            r->logLen = log_idx;
                        } else {
                            continue;
                        }
                    }
                    if (r->logLen >= r->logCap) {
                        int oldCap = r->logCap;
                        r->logCap *= 2;
                        r->log = realloc(r->log, r->logCap * sizeof(log_entry_t));
                        memset(r->log + oldCap, 0, (r->logCap - oldCap)*sizeof(log_entry_t));
                    }
                    r->log[r->logLen].term = e_term;
                    r->log[r->logLen].cmd = strdup(e_cmd);
                    r->logLen++;
                }
                if (num_entr > 0) lock_persisted(r);
                if (leader_commit > r->commitIdx) {
                    int last_new_entr = r->logLen -1;
                    r->commitIdx = (leader_commit < last_new_entr) ? leader_commit : last_new_entr;
                }
            }
        }
        cJSON_AddNumberToObject(reply, "term", r->curTerm);
        cJSON_AddNumberToObject(reply, "success", success);
        cJSON_AddNumberToObject(reply, "conflictTerm", conflict_term);
        cJSON_AddNumberToObject(reply, "conflictIndex", conflict_idx);
        pthread_mutex_unlock(&r->lock);
    }
    char* resp= cJSON_PrintUnformatted(reply);
    cJSON_Delete(args);
    cJSON_Delete(reply);
    return resp;
}