 /* $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_FILIO_H
/**
 * required for FIONREAD on solaris
 */
#include <sys/filio.h>
#endif

#ifndef _WIN32
#include <sys/ioctl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define ioctlsocket ioctl
#endif

#include <errno.h>
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#include "network-mysqld.h"
#include "network-mysqld-packet.h"
#include "chassis-event-thread.h"
#include "network-mysqld-lua.h"

#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"

/**
 * lua wrappers around the connection pool
 */

#define C(x) x, sizeof(x)-1
#define S(x) x->str, x->len

#define SELF_CONNECT_MSG_HANDLE(log_func, sock, msg, meet_error)          \
do {                                                                      \
        gchar *err = NULL;                                                \
        if (meet_error) {                                                 \
            err = g_strdup_printf(": %s(%d)", g_strerror(errno), errno);  \
        }                                                                 \
                                                                          \
        log_func("%s(%s): event_thread(%d) "                              \
                    "C:%s C_usr:%s C_db:%s S:%s(thread_id:%u) %s%s",      \
                    G_STRLOC, __func__, chassis_event_get_threadid(),     \
                    NETWORK_SOCKET_SRC_NAME(con->client),                 \
                    NETWORK_SOCKET_USR_NAME(con->client),                 \
                    NETWORK_SOCKET_DB_NAME(con->client),                  \
                    backend->addr->name->str,                             \
                    NETWORK_SOCKET_THREADID(sock),                        \
                    msg, err != NULL ? err : "");                         \
        if (err != NULL) g_free(err);                                     \
} while (0)


/**
 * handle the events of a idling server connection in the pool 
 *
 * make sure we know about connection close from the server side
 * - wait_timeout
 */
static void network_mysqld_con_idle_handle(int event_fd, short events, void *user_data) {
    network_connection_pool_entry *pool_entry = user_data;
    network_connection_pool *pool             = pool_entry->pool;

    if (events == EV_READ) {
        int b = -1;

        /**
         * @todo we have to handle the case that the server really sent us something
         *        up to now we just ignore it
         */
        if (ioctlsocket(event_fd, FIONREAD, &b)) {
            g_log_dbproxy(g_warning, "S:%s(thread_id:%u) Usr:%s"
                                        "ioctl(%d, FIONREAD, ...) failed: %s(%d)",
                                        NETWORK_SOCKET_DST_NAME(pool_entry->sock),
                                        NETWORK_SOCKET_THREADID(pool_entry->sock),
                                        NETWORK_SOCKET_USR_NAME(pool_entry->sock),
                                        event_fd, g_strerror(errno), errno);

        } else if (b != 0) {
            g_log_dbproxy(g_warning, "S:%s(thread_id:%u) Usr:%s "
                                       "ioctl(%d, FIONREAD, ...) said there is something to read from backend, oops: %d",
                                       NETWORK_SOCKET_DST_NAME(pool_entry->sock),
                                       NETWORK_SOCKET_THREADID(pool_entry->sock),
                                       NETWORK_SOCKET_USR_NAME(pool_entry->sock),
                                       event_fd, b);
        } else {
            /* the server decided to close the connection (wait_timeout, crash, ... )
             *
             * remove us from the connection pool and close the connection */
            network_connection_pool_remove(pool, pool_entry, REMOVE_SERVER_ABNORMAL); // not in lua, so lock like lua_lock
        }
    } else if  (events == EV_TIMEOUT) {
        network_connection_pool_remove(pool, pool_entry, REMOVE_IDLE_TIMEOUT);
    }
}

/**
 * move the con->server into connection pool and disconnect the 
 * proxy from its backend 
 */
int network_connection_pool_lua_add_connection(network_mysqld_con *con) {
    network_connection_pool_entry *pool_entry = NULL;
    network_mysqld_con_lua_t *st = con->plugin_con_state;
    guint64 ts_current = 0;
    gint db_connection_max_age = con->srv->db_connection_max_age;
    gint db_connection_idle_timeout = con->srv->db_connection_idle_timeout;

    /* con-server is already disconnected, got out */
    if (!con->server) return 0;

    /* TODO bug fix */
    /* when mysql return unkonw packet, response is null, insert the socket into pool cause segment fault. */
    /* ? should init socket->challenge  ? */
    /* if response is null, conn has not been authed, use an invalid username. */
    if(!con->server->response)
    {
        g_log_dbproxy(g_warning, "(remove) remove socket from pool, response is NULL, sock->src:%s, sock->dst:%s",
                        NETWORK_SOCKET_SRC_NAME(con->server),
                        NETWORK_SOCKET_DST_NAME(con->server));

        con->server->response = network_mysqld_auth_response_new();
        g_string_assign_len(con->server->response->username, C("mysql_proxy_invalid_user"));
    }

    /* the server connection is still authed */
    con->server->is_authed = 1;

    if (db_connection_max_age > 0 ) {
        ts_current = chassis_get_rel_milliseconds();

        if (ts_current > con->server->ts_connected + db_connection_max_age * 1000) {
            network_socket_free(con->server);
            goto ret;
        }
    }

    /* insert the server socket into the connection pool */
    network_connection_pool* pool = chassis_event_thread_pool(st->backend);
    pool_entry = network_connection_pool_add(pool, con->server);


    if (pool_entry) {
        gint timeout_s;

        if (db_connection_max_age > 0) {
            gint remaining_s = (db_connection_max_age * 1000 + con->server->ts_connected - ts_current + 999) / 1000;
            timeout_s = db_connection_idle_timeout > 0 ? MIN(remaining_s, db_connection_idle_timeout) : remaining_s;
        } else {
            timeout_s = db_connection_idle_timeout;
        }

        event_set(&(con->server->event), con->server->fd, EV_READ, network_mysqld_con_idle_handle, pool_entry);
        chassis_event_add_self(con->srv, &(con->server->event), timeout_s, 0); /* add a event, but stay in the same thread */
    }
    
ret:
    g_atomic_int_dec_and_test(&st->backend->connected_clients);
    st->backend = NULL;
    st->backend_ndx = -1;
    
    g_rw_lock_writer_lock(&con->server_lock);
    con->server = NULL;
    g_rw_lock_writer_unlock(&con->server_lock);

    return 0;
}

network_socket *self_connect(network_mysqld_con *con, network_backend_t *backend, GHashTable *pwd_table) {
    //1. connect DB
    network_socket *sock = network_socket_new(SOCKET_SERVER);
    network_address_copy(sock->dst, backend->addr);
    network_socket_set_chassis(sock, con->srv);

    chassis_event_thread_wait_start(con->srv, WAIT_EVENT_SERVER_CONNECT);
    con->wait_status = CON_WAIT_SERVER_CONNECT;

    if (-1 == (sock->fd = socket(sock->dst->addr.common.sa_family, sock->socket_type, 0))) {
        SELF_CONNECT_MSG_HANDLE(g_critical, sock, "socket failed", TRUE);
        network_socket_free(sock);
        return NULL;
    }

    if (-1 == (connect(sock->fd, &sock->dst->addr.common, sock->dst->len))) {
        SELF_CONNECT_MSG_HANDLE(g_critical, sock, "connecting to backend failed", TRUE);
        network_socket_free(sock);
        if (!IS_BACKEND_OFFLINE(backend) && !IS_BACKEND_WAITING_EXIT(backend)) {
            SET_BACKEND_STATE(backend, BACKEND_STATE_DOWN);
            g_log_dbproxy(g_warning, "event_thread(%d) set backend (%s) state to DOWN",
                                chassis_event_get_threadid(),
                                backend->addr->name->str);
        }
        return NULL;
    }

    //2. read handshake���ص��ǻ�ȡ20���ֽڵ����
    off_t to_read = NET_HEADER_SIZE;
    guint offset = 0;
    guchar header[NET_HEADER_SIZE];
    while (to_read > 0) {
        gssize len = recv(sock->fd, header + offset, to_read, 0);
        if (len == -1 || len == 0) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, "recv() failed when reading handshake package header", TRUE);
            network_socket_free(sock);
            return NULL;
        }
        offset += len;
        to_read -= len;
    }

    to_read = header[0] + (header[1] << 8) + (header[2] << 16);
    offset = 0;
    GString *data = g_string_sized_new(to_read);
    while (to_read > 0) {
        gssize len = recv(sock->fd, data->str + offset, to_read, 0);
        if (len == -1 || len == 0) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, "recv() failed when reading handshake package body", TRUE);
            network_socket_free(sock);
            g_string_free(data, TRUE);
            return NULL;
        }
        offset += len;
        to_read -= len;
    }
    data->len = offset;

    network_packet packet;
    packet.data = data;
    packet.offset = 0;
    network_mysqld_auth_challenge *challenge = network_mysqld_auth_challenge_new();

    if (network_mysqld_proto_get_auth_challenge(&packet, challenge)) {
        network_mysqld_err_packet_t *err_packet = network_mysqld_err_packet_new();
        packet.offset = 0;
        if (!network_mysqld_proto_get_err_packet(&packet, err_packet)) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, err_packet->errmsg->str, TRUE);
        }
        con->server_error_code = err_packet->errcode;
        network_mysqld_err_packet_free(err_packet);
        network_mysqld_auth_challenge_free(challenge);
        network_socket_free(sock);
        return NULL;
    }

    g_assert((con->client->response->capabilities
            & (CLIENT_COMPRESS | CLIENT_PLUGIN_AUTH)) == 0);

    g_assert((con->client->response->capabilities
            & (CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION)) != 0);

    //3. ���response
    GString *response = g_string_sized_new(20);
    GString *hashed_password = get_hash_passwd(pwd_table, NETWORK_SOCKET_USR_NAME(con->client),
                                                         &(con->srv->backends->user_mgr_lock));
    if (hashed_password) {
        network_mysqld_proto_password_scramble(response, S(challenge->challenge), S(hashed_password));
        g_string_free(hashed_password, TRUE);
    } else {
        SELF_CONNECT_MSG_HANDLE(g_critical, sock, "lookup hashed_password failed", TRUE);

        network_socket_free(sock);
        g_string_free(data, TRUE);
        network_mysqld_auth_challenge_free(challenge);
        g_string_free(response, TRUE);
        return NULL;
    }

    //4. send auth
    off_t to_write = 58 + con->client->response->username->len;
    if (con->client->response->capabilities & CLIENT_CONNECT_WITH_DB)
    {
        to_write += con->client->response->database->len + 1;
    }

    offset = 0;
    g_string_truncate(data, 0);
    network_mysqld_proto_append_int24(data, to_write - 4);
    network_mysqld_proto_append_int8(data, 1);
    network_mysqld_proto_append_int32(data, con->client->response->capabilities);
    network_mysqld_proto_append_int32(data, 0x1000000);
    network_mysqld_proto_append_int8(data, UTF8_CHARSET_INDEX); // 字符集后续会根据client中的charset来重新设置
    network_mysqld_proto_append_int_len(data, 0, 23);
    g_string_append_len(data, con->client->response->username->str, con->client->response->username->len);
    network_mysqld_proto_append_int8(data, 0x00);
    network_mysqld_proto_append_int8(data, 0x14);
    g_string_append_len(data, response->str, 20);
    if (con->client->response->capabilities & CLIENT_CONNECT_WITH_DB)
    {
        g_string_append(data, con->client->response->database->str);
        network_mysqld_proto_append_int8(data, 0x00);

        g_string_assign(sock->conn_attr.default_db, con->client->response->database->str);
    }
    g_string_assign(sock->conn_attr.charset_client, DEFAULT_DB_CONN_CHARSET_STR);
    g_string_assign(sock->conn_attr.charset_results, DEFAULT_DB_CONN_CHARSET_STR);
    g_string_assign(sock->conn_attr.charset_connection, DEFAULT_DB_CONN_CHARSET_STR);
    g_string_free(response, TRUE);
    while (to_write > 0) {
        gssize len = send(sock->fd, data->str + offset, to_write, 0);
        if (len == -1) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, "send response package failed", TRUE);

            network_socket_free(sock);
            g_string_free(data, TRUE);
            network_mysqld_auth_challenge_free(challenge);
            return NULL;
        }
        offset += len;
        to_write -= len;
    }

    //5. read auth result
    to_read = NET_HEADER_SIZE;
    offset = 0;
    while (to_read > 0) {
        gssize len = recv(sock->fd, header + offset, to_read, 0);
        if (len == -1 || len == 0) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, "recv() failed when reading auth_result package header", TRUE);

            network_socket_free(sock);
            g_string_free(data, TRUE);
            network_mysqld_auth_challenge_free(challenge);
            return NULL;
        }
        offset += len;
        to_read -= len;
    }

    to_read = header[0] + (header[1] << 8) + (header[2] << 16);
    offset = 0;
    g_string_truncate(data, 0);
    g_string_set_size(data, to_read);
    while (to_read > 0) {
        gssize len = recv(sock->fd, data->str + offset, to_read, 0);
        if (len == -1 || len == 0) {
            SELF_CONNECT_MSG_HANDLE(g_critical, sock, "recv() failed when reading auth_result package body", TRUE);

            network_socket_free(sock);
            g_string_free(data, TRUE);
            network_mysqld_auth_challenge_free(challenge);
            return NULL;
        }
        offset += len;
        to_read -= len;
    }
    data->len = offset;

    if (data->str[0] != MYSQLD_PACKET_OK) {
        SELF_CONNECT_MSG_HANDLE(g_critical, sock, "auth_result status isn't MYSQLD_PACKET_OK", TRUE);

        network_socket_free(sock);
        g_string_free(data, TRUE);
        network_mysqld_auth_challenge_free(challenge);
        return NULL;
    }
    g_string_free(data, TRUE);

    //6. set non-block
    network_socket_set_non_blocking(sock);
    network_socket_connect_setopts(sock);   //�˾��Ƿ���Ҫ���Ƿ�Ӧ�÷��ڵ�1��ĩβ��

    sock->challenge = challenge;
    sock->response = network_mysqld_auth_response_copy(con->client->response);

    sock->ts_connected = chassis_get_rel_milliseconds();

    guint64 long_wait_time = chassis_event_thread_wait_end(con->srv, WAIT_EVENT_SERVER_CONNECT);
    if (long_wait_time > 0) {
        gchar *msg = g_strdup_printf("long conn wait event: wait time(%d us)", long_wait_time);
        SELF_CONNECT_MSG_HANDLE(g_warning, sock, msg, FALSE);
        g_free(msg);
    }
    con->wait_status = CON_NO_WAIT;

    if (TRACE_CONNECTION_POOL(con->srv->log->log_trace_modules)) {
        GString *challenge_str = network_mysqld_auth_challenge_dump(sock->challenge);
        GString *auth_str = network_mysqld_auth_response_dump(sock->response);
        gchar *msg = g_strdup_printf("connection created, challenge:%s auth:%s",
                                    challenge_str->str, auth_str->str);
        SELF_CONNECT_MSG_HANDLE(g_message, sock, msg, FALSE);
        g_free(msg);
        g_string_free(challenge_str, TRUE);
        g_string_free(auth_str, TRUE);
    } else {
        SELF_CONNECT_MSG_HANDLE(g_message, sock, "create backend connection success", FALSE);
    }

    return sock;
}

/**
 * swap the server connection with a connection from
 * the connection pool
 *
 * we can only switch backends if we have a authed connection in the pool.
 *
 * @return NULL if swapping failed
 *         the new backend on success
 */
network_socket *network_connection_pool_lua_swap(network_mysqld_con *con, network_backend_t *backend, int backend_ndx, GHashTable *pwd_table) {
    network_socket *send_sock;
    network_mysqld_con_lua_t *st = con->plugin_con_state;

    if (backend == NULL) { return NULL; }

    /**
     * get a connection from the pool which matches our basic requirements
     * - username has to match
     * - default_db should match
     */
        
#ifdef DEBUG_CONN_POOL
    g_log_dbproxy(g_debug, "(swap) check if we have a connection for this user in the pool '%s'", con->client->response ? con->client->response->username->str: "empty_user");
#endif
    network_connection_pool* pool = chassis_event_thread_pool(backend);
    if (NULL == (send_sock = network_connection_pool_get(pool, con->client->response->username, con->client->response->capabilities, (void *)con))) {
        /**
         * no connections in the pool
         */
        if (TRACE_SQL(con->srv->log->log_trace_modules)) {
            gchar *msg = g_strdup_printf("backend(id:%d host:%s) pool have no connection of Usr: %s",
                                                backend_ndx, backend->addr->name->str,
                                                NETWORK_SOCKET_USR_NAME(con->client));
            CON_MSG_HANDLE(g_message, con, msg);
            g_free(msg);
        }
        
        if (NULL == (send_sock = self_connect(con, backend, pwd_table))) {
            st->backend_ndx = -1;
            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                gchar *msg = g_strdup_printf("create new connection to backend(id:%d host:%s) failed",
                                                            backend_ndx, backend->addr->name->str);
                CON_MSG_HANDLE(g_message, con, msg);
                g_free(msg);
            }
            return NULL;
        }
    }

    if (TRACE_SQL(con->srv->log->log_trace_modules)) {
        gchar *msg = g_strdup_printf("allocate connection(id:%d S:%s(thread_id:%u)) to client",
                                             backend_ndx,
                                             NETWORK_SOCKET_DST_NAME(send_sock),
                                             NETWORK_SOCKET_THREADID(send_sock));
        CON_MSG_HANDLE(g_message, con, msg);
        g_free(msg);
    }

    /* connect to the new backend */
    st->backend = backend;
    st->backend_ndx = backend_ndx;

    return send_sock;
}

