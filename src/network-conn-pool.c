/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ */
 

#include <glib.h>

#include "network-conn-pool.h"
#include "network-mysqld-packet.h"
#include "glib-ext.h"
#include "sys-pedantic.h"

#define CONNECTION_POOL_HASH_BUCKETS 8

#define RM_SOCK_MSG_HANDLE(log_func, msg)                           \
do {                                                                \
    log_func("%s(%s): remove connection(S:%s(thread_id:%u)) in pool(%p Usr:%s host:%s) for %s", \
                    G_STRLOC, __func__,                             \
                    NETWORK_SOCKET_DST_NAME(sock),                  \
                    NETWORK_SOCKET_THREADID(sock),                  \
                    pool,                                           \
                    NETWORK_SOCKET_USR_NAME(sock),                  \
                    NETWORK_SOCKET_DST_NAME(sock), msg);            \
} while (0)



/** @file
 * connection pools
 *
 * in the pool we manage idle connections
 * - keep them up as long as possible
 * - make sure we don't run out of seconds
 * - if the client is authed, we have to pick connection with the same user
 * - ...  
 */

/**
 * create a empty connection pool entry
 *
 * @return a connection pool entry
 */
network_connection_pool_entry *network_connection_pool_entry_new(void) {
    network_connection_pool_entry *e;

    e = g_new0(network_connection_pool_entry, 1);

    return e;
}

/**
 * free a conn pool entry
 *
 * @param e the pool entry to free
 * @param free_sock if true, the attached server-socket will be freed too
 */
void network_connection_pool_entry_free(network_connection_pool_entry *e, gboolean free_sock) {
    if (!e) return;

    if (e->sock && free_sock) {
        network_socket *sock = e->sock;

        event_del(&(sock->event));
        network_socket_free(e->sock);
    }

    g_free(e);
}

static guint network_connection_pool_hash_func(GString *sock) {
    return g_string_hash(sock) % CONNECTION_POOL_HASH_BUCKETS;
}

static gboolean network_connection_pool_equal_func(GString *sock1, GString *sock2) {
    return g_string_equal(sock1, sock2);
}

static void network_connection_pool_key_free(GString *sock) {
    g_string_free(sock, TRUE);
}

static void network_connection_pool_socket_free(network_connection_pool_entry *entry) {
    network_connection_pool_entry_free(entry, TRUE);
}

static void network_connection_pool_value_free(GQueue *entry_list) {
    g_queue_free_full(entry_list, (GDestroyNotify)network_connection_pool_socket_free);
}

/**
 * init a connection pool
 */
network_connection_pool *network_connection_pool_new(void) {
    network_connection_pool *pool = g_hash_table_new_full((GHashFunc)network_connection_pool_hash_func,
            (GEqualFunc)network_connection_pool_equal_func,
            (GDestroyNotify)network_connection_pool_key_free,
            (GDestroyNotify)network_connection_pool_value_free);

    return pool;
}

/**
 * free all entries of the pool
 *
 */
void network_connection_pool_free(network_connection_pool *pool) {
    g_hash_table_destroy(pool);
}

/**
 * get a connection from the pool
 *
 * make sure we have at lease <min-conns> for each user
 * if we have more, reuse a connect to reauth it to another user
 *
 * @param pool connection pool to get the connection from
 * @param username (optional) name of the auth connection
 * @param default_db (unused) unused name of the default-db
 */
network_socket *network_connection_pool_get(network_connection_pool *pool,
                    GString *user_name, guint32 capabilities, void *userdata) {
    network_connection_pool_entry *entry = NULL;
    network_mysqld_con *con = (network_mysqld_con *)userdata;
    GQueue *entry_list = NULL;
    GString *hash_key = g_string_sized_new(user_name->len + 4);

    network_mysqld_proto_append_int32(hash_key, capabilities);
    g_string_append_len(hash_key, user_name->str, user_name->len);

    entry_list = g_hash_table_lookup(pool, hash_key);

    /**
     * if we know this use, return a authed connection 
     */

    if (!entry_list)
    {
        if (TRACE_CONNECTION_POOL(con->srv->log->log_trace_modules)) {
        g_log_dbproxy(g_message, "event_thread(%d) pool(%p Usr:%s) has no entry list for C:%s",
                        chassis_event_get_threadid(), pool,
                        user_name->str, NETWORK_SOCKET_SRC_NAME(con->client));


        }
        g_string_free(hash_key, TRUE);
        return NULL;
    }

    entry = g_queue_pop_tail(entry_list);

    if (entry_list->length == 0)
    {
        if (TRACE_CONNECTION_POOL(con->srv->log->log_trace_modules)) {
            g_log_dbproxy(g_message, "event_thread(%d) after allocated connection(%s(thread_id:%u)) in pool(%p Usr:%s S:%s) remaining no connection", chassis_event_get_threadid(), NETWORK_SOCKET_DST_NAME(entry->sock), NETWORK_SOCKET_THREADID(entry->sock), pool, user_name->str, NETWORK_SOCKET_DST_NAME(entry->sock));
        }
        g_hash_table_remove(pool, hash_key);
    } else {
        if (TRACE_CONNECTION_POOL(con->srv->log->log_trace_modules)) {
            g_log_dbproxy(g_message, "event_thread(%d) after allocated connection(%s(thread_id:%u)) in pool(%p Usr:%s S:%s) remaining %d connections", chassis_event_get_threadid(), NETWORK_SOCKET_DST_NAME(entry->sock), NETWORK_SOCKET_THREADID(entry->sock), pool, user_name->str, NETWORK_SOCKET_DST_NAME(entry->sock), g_queue_get_length(entry_list));
        }
    }

    network_socket *sock = entry->sock;
    network_connection_pool_entry_free(entry, FALSE);

    /* remove the idle handler from the socket */   
    event_del(&(sock->event));
        
    g_string_free(hash_key, TRUE);
    return sock;
}

/**
 * add a connection to the connection pool
 *
 */
network_connection_pool_entry *network_connection_pool_add(network_connection_pool *pool, network_socket *sock) {
    if (pool) {
        network_connection_pool_entry *entry = network_connection_pool_entry_new();

        if (entry) {
            entry->sock = sock;
            entry->pool = pool;
            chassis *srv = (chassis *)sock->srv;

            GQueue *entry_list = NULL;
            GString *hash_key = g_string_sized_new(sock->response->username->len + 4);

            network_mysqld_proto_append_int32(hash_key, sock->response->capabilities);
            g_string_append_len(hash_key, sock->response->username->str, sock->response->username->len);

            entry_list = g_hash_table_lookup(pool, hash_key);

            if (entry_list != NULL) {
                g_queue_push_tail(entry_list, entry);
                g_string_free(hash_key, TRUE);
            } else {
                entry_list = g_queue_new();
                g_queue_push_tail(entry_list, entry);
                g_hash_table_insert(pool, hash_key, entry_list);
            }

            if (TRACE_CONNECTION_POOL(srv->log->log_trace_modules)) {
                g_log_dbproxy(g_message, "event_thread(%d) added connection(S:%s(thread_id:%u)) to pool(%p Usr:%s S:%s), the pool size is %d now", chassis_event_get_threadid(), NETWORK_SOCKET_DST_NAME(sock), NETWORK_SOCKET_THREADID(sock), pool, NETWORK_SOCKET_USR_NAME(sock), NETWORK_SOCKET_DST_NAME(sock), g_queue_get_length(entry_list));
            }

            return entry;
        }
    }

    network_socket_free(sock);
    return NULL;
}

/**
 * remove the connection referenced by entry from the pool 
 */
void network_connection_pool_remove(network_connection_pool *pool, network_connection_pool_entry *entry, gint remove_type) {
    network_socket *sock = entry->sock;

    if (sock->response == NULL) {
        g_log_dbproxy(g_warning, "remove backend from pool failed, "
                                    "response is NULL, src is %s, dst is %s",
                        NETWORK_SOCKET_SRC_NAME(sock),
                        NETWORK_SOCKET_DST_NAME(sock));
    }

    if (remove_type == REMOVE_IDLE_TIMEOUT) {
        RM_SOCK_MSG_HANDLE(g_warning, "closed by dbproxy due to dbconnection_idle_timeout");
    } else if (remove_type == REMOVE_SERVER_ABNORMAL) {
        RM_SOCK_MSG_HANDLE(g_critical, "closed by remote server(timeout, crash)");
    }

    GQueue *entry_list = NULL;
    GString *hash_key = g_string_sized_new(sock->response->username->len + 4);

    network_mysqld_proto_append_int32(hash_key, sock->response->capabilities);
    g_string_append_len(hash_key, sock->response->username->str, sock->response->username->len);

    entry_list = g_hash_table_lookup(pool, hash_key);

    g_assert (entry_list->length > 0);

    g_queue_remove(entry_list, entry);

    if (entry_list->length == 0)
    {
        g_hash_table_remove(pool, hash_key);
    }

    g_string_free(hash_key, TRUE);
    network_connection_pool_entry_free(entry, TRUE);
}
