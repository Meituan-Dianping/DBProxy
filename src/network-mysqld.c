/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

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

#include <sys/types.h>

#ifdef HAVE_SYS_FILIO_H
#include <sys/filio.h> /* required for FIONREAD on solaris */
#endif

#ifndef _WIN32
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <arpa/inet.h> /** inet_ntoa */
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <netdb.h>
#include <unistd.h>
#else
#include <winsock2.h>
#include <io.h>
#define ioctl ioctlsocket
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#include <glib.h>

#include <mysql.h>
#include <mysqld_error.h>

#include "network-debug.h"
#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "network-conn-pool.h"
#include "chassis-mainloop.h"
#include "chassis-event-thread.h"
#include "lua-scope.h"
#include "glib-ext.h"
#include "network-mysqld-lua.h"
#include "chassis-log.h"

#if defined(HAVE_SYS_SDT_H) && defined(ENABLE_DTRACE)
#include <sys/sdt.h>
#include "proxy-dtrace-provider.h"
#else
#include "disable-dtrace.h"
#endif

#ifdef HAVE_WRITEV
#define USE_BUFFERED_NETIO 
#else
#undef USE_BUFFERED_NETIO 
#endif

#ifdef _WIN32
#define E_NET_CONNRESET WSAECONNRESET
#define E_NET_CONNABORTED WSAECONNABORTED
#define E_NET_WOULDBLOCK WSAEWOULDBLOCK
#define E_NET_INPROGRESS WSAEINPROGRESS
#else
#define E_NET_CONNRESET ECONNRESET
#define E_NET_CONNABORTED ECONNABORTED
#define E_NET_INPROGRESS EINPROGRESS
#if EWOULDBLOCK == EAGAIN
/**
 * some system make EAGAIN == EWOULDBLOCK which would lead to a 
 * error in the case handling
 *
 * set it to -1 as this error should never happen
 */
#define E_NET_WOULDBLOCK -1
#else
#define E_NET_WOULDBLOCK EWOULDBLOCK
#endif
#endif

extern int parse_resultset_fields(proxy_resultset_t *res);

const char *network_mysqld_stat_desc[PROXY_STAT_END] =
{
    "Com_query_read",
    "Com_query_write",
    "Com_other",
    "Com_query_read_sent",
    "Com_query_write_sent",
    "Com_other_sent",
    "Slow_query",
    "Slow_query_total_time",
    "Net_server_read",
    "Net_server_write",
    "Net_client_read",
    "Net_client_write",
    "Net_server_read_packets",
    "Net_server_write_packets",
    "Net_client_read_packets",
    "Net_client_write_packets",
    "Event_waiting",
    "Threads_running",
    "Invalid",              // THREAD_STAT_END
    "Max_connections",
    "Max_used_connections",
    "Connections",
    "DB_connections",
    "DB_connections_cached",
    "Attempted_connects",
    "Aborted_connects",
    "Closed_clients",
    "Aborted_clients",
    "Invalid",              // GLOBAL_STAT_END
    "Percentile"
};

const char *network_mysqld_conn_wait_desc[CON_WAIT_END] =
{
    "Running",
    "Waiting client socket read",
    "Waiting client socket write",
    "Waiting server socket read",
    "Waiting server socket write"
    "Waiting server connect"
};

const char *chas_thread_wait_event_desc[WAIT_EVENT_END] =
{
    "Waiting server connect"
};

const char *network_mysqld_conn_stat_desc[CON_STATE_SEND_LOCAL_INFILE_RESULT+1] =
{
    "CON_STATE_INIT",               /**< A new client connection was established */
    "CON_STATE_CONNECT_SERVER",     /**< A connection to a backend is about to be made */
    "CON_STATE_READ_HANDSHAKE",        /**< A handshake packet is to be read from a server */
    "CON_STATE_SEND_HANDSHAKE",        /**< A handshake packet is to be sent to a client */
    "CON_STATE_READ_AUTH",             /**< An authentication packet is to be read from a client */
    "CON_STATE_SEND_AUTH",             /**< An authentication packet is to be sent to a server */
    "CON_STATE_READ_AUTH_RESULT",      /**< The result of an authentication attempt is to be read from a server */
    "CON_STATE_SEND_AUTH_RESULT",      /**< The result of an authentication attempt is to be sent to a client */
    "CON_STATE_READ_AUTH_OLD_PASSWORD",/**< The authentication method used is for pre-4.1 MySQL servers, internal state */
    "CON_STATE_SEND_AUTH_OLD_PASSWORD",/**< The authentication method used is for pre-4.1 MySQL servers, internal state */
    "CON_STATE_READ_QUERY" ,           /**< COM_QUERY packets are to be read from a client */
    "CON_STATE_SEND_QUERY",           /**< COM_QUERY packets are to be sent to a server */
    "CON_STATE_READ_QUERY_RESULT",    /**< Result set packets are to be read from a server */
    "CON_STATE_SEND_QUERY_RESULT",    /**< Result set packets are to be sent to a client */

    "CON_STATE_CLOSE_CLIENT",         /**< The client connection should be closed */
    "CON_STATE_SEND_ERROR",           /**< An unrecoverable error occurred, leads to sending a MySQL ERR packet to the client and closing the client connection */
    "CON_STATE_ERROR",                /**< An error occurred (malformed/unexpected packet, unrecoverable network error), internal state */

    "CON_STATE_CLOSE_SERVER",         /**< The server connection should be closed */

    /* handling the LOAD DATA LOCAL INFILE protocol extensions */
    "CON_STATE_READ_LOCAL_INFILE_DATA",
    "CON_STATE_SEND_LOCAL_INFILE_DATA",
    "CON_STATE_READ_LOCAL_INFILE_RESULT",
    "CON_STATE_SEND_LOCAL_INFILE_RESULT"
};


const char *network_mysqld_query_type_desc[QUERY_TYPE_NUM] =
{
    "read",
    "write",
    "other"
};

#define EMIT_WAIT_FOR_EVENT(ev_struct, ev_type, con)                \
do {                                                                \
    gchar *event_msg = NULL, *event_obj = NULL;                     \
    if (ev_struct == con->client) {                                 \
        event_obj = "client";                                       \
    } else if (ev_struct == con->server) {                          \
        event_obj = "backend";                                      \
    }                                                               \
    if (event_obj != NULL) {                                        \
        event_msg = g_strdup_printf("wait for %s event(type:%d,fd:%d)",     \
                                     event_obj, ev_type, ev_struct->fd);    \
        CON_MSG_HANDLE(g_message, con, event_msg);                          \
        g_free(event_msg);                                                  \
    }                                                                       \
} while (0)


static info_func *info_funcs_new(gint info_func_nums);
static void info_funcs_free(info_func *info_funcs);

/**
 * a handy marco for constant strings 
 */
#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

//static GMutex con_mutex;

/**
 * call the cleanup callback for the current connection
 *
 * @param srv    global context
 * @param con    connection context
 *
 * @return       NETWORK_SOCKET_SUCCESS on success
 */
network_socket_retval_t plugin_call_cleanup(chassis *srv, network_mysqld_con *con) {
    NETWORK_MYSQLD_PLUGIN_FUNC(func) = NULL;
    network_socket_retval_t retval = NETWORK_SOCKET_SUCCESS;

    func = con->plugins.con_cleanup;
    
    if (!func) return retval;

//  LOCK_LUA(srv->priv->sc);    /*remove lock*/
    retval = (*func)(srv, con);
//  UNLOCK_LUA(srv->priv->sc);  /*remove lock*/

    return retval;
}

int network_mysqld_init(chassis *srv, gchar *default_file, guint reomve_backend_timeout) {
    /* store the pointer to the chassis in the Lua registry */
    srv->sc = lua_scope_new();
    lua_State *L = srv->sc->L;
    lua_pushlightuserdata(L, (void*)srv);
    lua_setfield(L, LUA_REGISTRYINDEX, CHASSIS_LUA_REGISTRY_KEY);

    srv->backends = network_backends_new(srv->event_thread_count, default_file);
    srv->backends->remove_backend_timeout = reomve_backend_timeout;

    return 0;
}

set_var_unit *set_var_new()
{
    set_var_unit *set_var = g_new0(set_var_unit, 1);

    set_var->var_name = g_string_new(NULL);
    set_var->var_value = g_string_new(NULL);
    set_var->var_value_extra = 0;

    return set_var;
}

gint set_var_name_compare(set_var_unit *a, set_var_unit *b, void *user_data)
{
    return g_ascii_strcasecmp(a->var_name->str, b->var_name->str);
}

gboolean set_var_value_eq(set_var_unit *a, set_var_unit *b)
{
    return (a->var_value_extra == b->var_value_extra) && (g_ascii_strcasecmp(a->var_value->str, b->var_value->str) == 0);
}

gint set_var_name_ge(set_var_unit *a, set_var_unit *b)
{
    return (g_ascii_strcasecmp(a->var_name->str, b->var_name->str) >= 0) ? 0 : 1;
}

set_var_unit * set_var_copy(set_var_unit *set_var, void *data)
{
    if(set_var == NULL) return NULL;

    set_var_unit *new_set_var = set_var_new();

    g_assert(set_var->var_name != NULL);
    g_assert(set_var->var_value != NULL);

    g_string_assign(new_set_var->var_name, set_var->var_name->str);
    g_string_assign(new_set_var->var_value, set_var->var_value->str);
    new_set_var->var_value_extra = set_var->var_value_extra;

    return new_set_var;
}

// 此处简化处理，单个标示符外只支持负数
void set_var_print_set_value(GString *data, set_var_unit *set_var, gboolean set_default)
{
    if (set_default) {
        g_string_append_printf(data, "%s= %s", set_var->var_name->str, "default");
    }else if (set_var->var_value_extra & VALUE_IS_STRING) {
        g_string_append_printf(data, "%s= \'%s\'", set_var->var_name->str, set_var->var_value->str);
    } else if (set_var->var_value_extra & VALUE_IS_MINUS) {
        g_string_append_printf(data, "%s= -%s", set_var->var_name->str, set_var->var_value->str);
    } else {
        g_string_append_printf(data, "%s= %s", set_var->var_name->str, set_var->var_value->str);
    }
}

void set_var_free(set_var_unit *set_var)
{
    if(set_var == NULL) return;

    g_assert(set_var->var_name != NULL);
    g_assert(set_var->var_value != NULL);

    g_string_free(set_var->var_name, TRUE);
    g_string_free(set_var->var_value, TRUE);

    g_free(set_var);

    return;
}

void set_var_queue_insert(GQueue *set_vars, gchar *set_var_name, gchar *set_var_value, guint set_var_value_extra)
{
    set_var_unit *set_var = set_var_new();
    g_string_assign(set_var->var_name, set_var_name);
    g_string_assign(set_var->var_value, set_var_value);
    set_var->var_value_extra = set_var_value_extra;

    // 找到第一个大于等于当前set_var的节点
    GList *insert_node = g_queue_find_custom(set_vars, set_var, (GCompareFunc)set_var_name_ge);
    if (insert_node != NULL)
    {
        if (set_var_name_compare(insert_node->data, set_var, NULL) == 0)
        {
            set_var_unit *set_var_equal = insert_node->data;
            g_string_truncate(set_var_equal->var_value, 0);
            g_string_assign(set_var_equal->var_value, set_var_value);
            set_var_equal->var_value_extra = set_var_value_extra;
            set_var_free(set_var);
        }
        else
        {
            g_queue_insert_before(set_vars, insert_node, set_var);
        }
    }
    else
    {
        g_queue_push_tail(set_vars, set_var);
    }
}

void set_var_queue_merge(GQueue *set_vars, GQueue *to_merge_set_vars)
{
    GList *cur_set_vars = g_queue_peek_head_link(set_vars);
    GList *cur_to_merge_set_vars = g_queue_peek_head_link(to_merge_set_vars);
    gint cmp_result = -1;
    set_var_unit *set_var = NULL;

    if (cur_set_vars == NULL && cur_to_merge_set_vars == NULL) return;

    do {
        if (cur_set_vars != NULL && cur_to_merge_set_vars != NULL) {
            cmp_result = set_var_name_compare(cur_to_merge_set_vars->data, cur_set_vars->data, NULL);

            if (cmp_result == 0) {
                if (!set_var_value_eq(cur_set_vars->data, cur_to_merge_set_vars->data))
                {
                    set_var = cur_set_vars->data;
                    g_string_truncate(set_var->var_value, 0);
                    g_string_assign(set_var->var_value, ((set_var_unit *)cur_to_merge_set_vars->data)->var_value->str);
                    set_var->var_value_extra = ((set_var_unit *)cur_to_merge_set_vars->data)->var_value_extra;
                }
                cur_set_vars = g_list_next(cur_set_vars);
                cur_to_merge_set_vars = g_list_next(cur_to_merge_set_vars);
                break;
            } else if (cmp_result < 0) {
                g_queue_insert_before(set_vars, cur_set_vars, set_var_copy(cur_to_merge_set_vars->data, NULL));
                cur_to_merge_set_vars = g_list_next(cur_to_merge_set_vars);
                break;
            } else {
                cur_set_vars = g_list_next(cur_set_vars);
            }
        } else if (cur_to_merge_set_vars != NULL) {
             g_queue_push_tail(set_vars, set_var_copy(cur_to_merge_set_vars->data, NULL));
            cur_to_merge_set_vars = g_list_next(cur_to_merge_set_vars);
        }
    } while (cur_to_merge_set_vars != NULL);
}

/**
 * create a connection 
 *
 * @return       a connection context
 */
network_mysqld_con *network_mysqld_con_new() {
    network_mysqld_con *con;

    con = g_new0(network_mysqld_con, 1);
    con->parse.command = -1;

    con->conn_status.set_charset_client     = g_string_new(NULL);
    con->conn_status.set_charset_results    = g_string_new(NULL);
    con->conn_status.set_charset_connection = g_string_new(NULL);
    con->conn_status.lock_key               = g_string_new(NULL);
    con->conn_status.use_db                 = g_string_new(NULL);
    con->conn_status.exit_phase             = CON_ALIVE_NORMAL;

    con->conn_status.info_funcs = info_funcs_new(INFO_FUNC_MAX);

    memset(con->conn_status_var.cur_query, 0, STMT_LENTH);
    con->conn_status_var.cur_query_com_type = -1;       

    con->locks = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

    con->merge_res = g_new(merge_res_t, 1);
    con->merge_res->rows = g_ptr_array_new();

    con->challenge = g_string_sized_new(20);

    con->con_filter_var.cur_sql_rewrite = NULL;
    con->con_filter_var.cur_sql_rewrite_md5 = NULL;
    con->con_filter_var.ts_read_query = 0;

    con->is_in_wait = FALSE;
    con->try_send_query_times = 0;

    con->server_error_code = 0;

    g_rw_lock_init(&con->server_lock);
    return con;
}

void network_mysqld_add_connection(chassis *srv, network_mysqld_con *con) {
    con->srv = srv;
/*
    g_mutex_lock(&con_mutex);
    g_ptr_array_add(srv->priv->cons, con);
    g_mutex_unlock(&con_mutex);
*/
}

/**
 * free a connection 
 *
 * closes the client and server sockets 
 *
 * @param con    connection context
 */
void network_mysqld_con_free(network_mysqld_con *con) {
    if (!con) return;

    if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
        CON_MSG_HANDLE(g_message, con, "connection closed");
    }

    if (con->parse.data && con->parse.data_free) {
        con->parse.data_free(con->parse.data);
    }

    if (con->server) {
        CON_MSG_HANDLE(g_message, con, "backend connection released by connection free");
        network_socket_free(con->server);
    }
    if (con->client) network_socket_free(con->client);

    /* we are still in the conns-array */
/*
    g_mutex_lock(&con_mutex);
    g_ptr_array_remove_fast(con->srv->priv->cons, con);
    g_mutex_unlock(&con_mutex);
*/

    if(con->conn_status_var.query_running)
    {
        thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);
        g_atomic_pointer_add(&(thread_status_var->thread_stat[THREAD_STAT_THREADS_RUNNING]), -1);
    }

    g_string_free(con->conn_status.set_charset_client, TRUE);
    g_string_free(con->conn_status.set_charset_results, TRUE);
    g_string_free(con->conn_status.set_charset_connection, TRUE);
    g_string_free(con->conn_status.lock_key, TRUE);
    g_string_free(con->conn_status.use_db, TRUE);
    info_funcs_free(con->conn_status.info_funcs);

    if (con->conn_status.set_vars != NULL) {
        g_queue_free_full(con->conn_status.set_vars, (GDestroyNotify)set_var_free);
    }

    g_hash_table_remove_all(con->locks);
    g_hash_table_destroy(con->locks);

    if (con->merge_res) {
        GPtrArray* rows = con->merge_res->rows;
        if (rows) {
            guint i;
            for (i = 0; i < rows->len; ++i) {
                GPtrArray* row = g_ptr_array_index(rows, i);
                guint j;
                for (j = 0; j < row->len; ++j) {
                    g_free(g_ptr_array_index(row, j));
                }
                g_ptr_array_free(row, TRUE);
            }
            g_ptr_array_free(rows, TRUE);
        }
        g_free(con->merge_res);
    }

    if (con->challenge) g_string_free(con->challenge, TRUE);

    if (con->con_filter_var.cur_sql_rewrite)
        g_string_free(con->con_filter_var.cur_sql_rewrite, TRUE);
    if (con->con_filter_var.cur_sql_rewrite_md5)
        g_string_free(con->con_filter_var.cur_sql_rewrite_md5, TRUE);

    g_rw_lock_clear(&con->server_lock);

    g_free(con);
}

#if 0 
static void dump_str(const char *msg, const unsigned char *s, size_t len) {
    GString *hex;
    size_t i;
        
        hex = g_string_new(NULL);

    for (i = 0; i < len; i++) {
        g_string_append_printf(hex, "%02x", s[i]);

        if ((i + 1) % 16 == 0) {
            g_string_append(hex, "\n");
        } else {
            g_string_append_c(hex, ' ');
        }

    }

    g_log_dbproxy(g_message, "(%s): %s", msg, hex->str);

    g_string_free(hex, TRUE);
}
#endif

int network_mysqld_queue_reset(network_socket *sock) {
    sock->packet_id_is_reset = TRUE;

    return 0;
}

/**
 * synchronize the packet-ids of two network-sockets 
 */
int network_mysqld_queue_sync(network_socket *dst, network_socket *src) {
    g_assert_cmpint(src->packet_id_is_reset, ==, FALSE);

    if (dst->packet_id_is_reset == FALSE) {
        /* this shouldn't really happen */
    }

    dst->last_packet_id = src->last_packet_id - 1;

    return 0;
}

/**
 * appends a raw MySQL packet to the queue 
 *
 * the packet is append the queue directly and shouldn't be used by the caller afterwards anymore
 * and has to by in the MySQL Packet format
 *
 */
int network_mysqld_queue_append_raw(network_socket *sock, network_queue *queue, GString *data) {
    guint32 packet_len;
    guint8  packet_id;

    /* check that the length header is valid */
    if (queue != sock->send_queue &&
        queue != sock->recv_queue) {
        g_log_dbproxy(g_warning, "queue = %p doesn't belong to sock %p(sock->src:%s sock->dst:%s)",
                (void *)queue, (void *)sock,
                NETWORK_SOCKET_SRC_NAME(sock), NETWORK_SOCKET_DST_NAME(sock));
        return -1;
    }

    g_assert_cmpint(data->len, >=, 4);

    packet_len = network_mysqld_proto_get_packet_len(data);
    packet_id  = network_mysqld_proto_get_packet_id(data);

    g_assert_cmpint(packet_len, ==, data->len - 4);

    if (sock->packet_id_is_reset) {
        /* the ->last_packet_id is undefined, accept what we get */
        sock->last_packet_id = packet_id;
        sock->packet_id_is_reset = FALSE;
    } else if (packet_id != (guint8)(sock->last_packet_id + 1)) {
        sock->last_packet_id++;
#if 0
        g_log_dbproxy(g_warning, "packet-id %d doesn't match for socket's last packet %d, patching it",
                packet_id,
                sock->last_packet_id);
#endif
        network_mysqld_proto_set_packet_id(data, sock->last_packet_id);
    } else {
        sock->last_packet_id++;
    }

    network_queue_append(queue, data);

    return 0;
}

/**
 * appends a payload to the queue
 *
 * the packet is copied and prepened with the mysql packet header before it is appended to the queue
 * if neccesary the payload is spread over multiple mysql packets
 */
int network_mysqld_queue_append(network_socket *sock, network_queue *queue, const char *data, size_t packet_len) {
    gsize packet_offset = 0;

    do {
        GString *s;
        gsize cur_packet_len = MIN(packet_len, PACKET_LEN_MAX);

        s = g_string_sized_new(packet_len + 4);

        if (sock->packet_id_is_reset) {
            sock->packet_id_is_reset = FALSE;
            sock->last_packet_id = 0xff; /** the ++last_packet_id will make sure we send a 0 */
        }

        network_mysqld_proto_append_packet_len(s, cur_packet_len);
        network_mysqld_proto_append_packet_id(s, ++sock->last_packet_id);
        g_string_append_len(s, data + packet_offset, cur_packet_len);

        network_queue_append(queue, s);

        if (packet_len == PACKET_LEN_MAX) {
            s = g_string_sized_new(4);

            network_mysqld_proto_append_packet_len(s, 0);
            network_mysqld_proto_append_packet_id(s, ++sock->last_packet_id);

            network_queue_append(queue, s);
        }

        packet_len -= cur_packet_len;
        packet_offset += cur_packet_len;
    } while (packet_len > 0);

    return 0;
}


/**
 * create a OK packet and append it to the send-queue
 *
 * @param con             a client socket 
 * @param affected_rows   affected rows 
 * @param insert_id       insert_id 
 * @param server_status   server_status (bitfield of SERVER_STATUS_*) 
 * @param warnings        number of warnings to fetch with SHOW WARNINGS 
 * @return 0
 *
 * @todo move to network_mysqld_proto
 */
int network_mysqld_con_send_ok_full(network_socket *con, guint64 affected_rows, guint64 insert_id, guint16 server_status, guint16 warnings ) {
    GString *packet = g_string_new(NULL);
    network_mysqld_ok_packet_t *ok_packet;

    ok_packet = network_mysqld_ok_packet_new();
    ok_packet->affected_rows = affected_rows;
    ok_packet->insert_id     = insert_id;
    ok_packet->server_status = server_status;
    ok_packet->warnings      = warnings;

    network_mysqld_proto_append_ok_packet(packet, ok_packet);
    
    network_mysqld_queue_append(con, con->send_queue, S(packet));
    network_mysqld_queue_reset(con);

    g_string_free(packet, TRUE);
    network_mysqld_ok_packet_free(ok_packet);

    return 0;
}

/**
 * send a simple OK packet
 *
 * - no affected rows
 * - no insert-id
 * - AUTOCOMMIT
 * - no warnings
 *
 * @param con             a client socket 
 */
int network_mysqld_con_send_ok(network_socket *con) {
    return network_mysqld_con_send_ok_full(con, 0, 0, SERVER_STATUS_AUTOCOMMIT, 0);
}

static int network_mysqld_con_send_error_full_all(network_socket *con,
        const char *errmsg, gsize errmsg_len,
        guint errorcode,
        const gchar *sqlstate,
        gboolean is_41_protocol) {
    GString *packet;
    network_mysqld_err_packet_t *err_packet;

    packet = g_string_sized_new(10 + errmsg_len);
    
    err_packet = is_41_protocol ? network_mysqld_err_packet_new() : network_mysqld_err_packet_new_pre41();
    err_packet->errcode = errorcode;
    if (errmsg) g_string_assign_len(err_packet->errmsg, errmsg, errmsg_len);
    if (sqlstate) g_string_assign_len(err_packet->sqlstate, sqlstate, strlen(sqlstate));

    network_mysqld_proto_append_err_packet(packet, err_packet);

    network_mysqld_queue_append(con, con->send_queue, S(packet));
    network_mysqld_queue_reset(con);

    network_mysqld_err_packet_free(err_packet);
    g_string_free(packet, TRUE);

    return 0;
}

/**
 * send a error packet to the client connection
 *
 * @note the sqlstate has to match the SQL standard. If no matching SQL state is known, leave it at NULL
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 * @param errorcode   mysql error-code we want to send
 * @param sqlstate    if none-NULL, 5-char SQL state to send, if NULL, default SQL state is used
 *
 * @return 0 on success
 */
int network_mysqld_con_send_error_full_nolog(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode, const gchar *sqlstate) {
    return network_mysqld_con_send_error_full_all(con, errmsg, errmsg_len, errorcode, sqlstate, TRUE);
}

/**
 * send a error-packet to the client connection
 *
 * errorcode is 1000, sqlstate is NULL
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 *
 * @see network_mysqld_con_send_error_full
 */
int network_mysqld_con_send_error(network_socket *con, const char *errmsg, gsize errmsg_len) {
    return network_mysqld_con_send_error_full_all(con, errmsg, errmsg_len, ER_UNKNOWN_ERROR, NULL, TRUE);
}

/**
 * send a error packet to the client connection (pre-4.1 protocol)
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 * @param errorcode   mysql error-code we want to send
 *
 * @return 0 on success
 */
int network_mysqld_con_send_error_pre41_full(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode) {
    return network_mysqld_con_send_error_full_all(con, errmsg, errmsg_len, errorcode, NULL, FALSE);
}

/**
 * send a error-packet to the client connection (pre-4.1 protocol)
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 *
 * @see network_mysqld_con_send_error_pre41_full
 */
int network_mysqld_con_send_error_pre41(network_socket *con, const char *errmsg, gsize errmsg_len) {
    return network_mysqld_con_send_error_pre41_full(con, errmsg, errmsg_len, ER_UNKNOWN_ERROR);
}


/**
 * get a full packet from the raw queue and move it to the packet queue 
 */
network_socket_retval_t network_mysqld_con_get_packet(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    GString *packet = NULL;
    GString header;
    char header_str[NET_HEADER_SIZE + 1] = "";
    guint32 packet_len;
    guint8  packet_id;

    /** 
     * read the packet header (4 bytes)
     */
    header.str = header_str;
    header.allocated_len = sizeof(header_str);
    header.len = 0;

    /* read the packet len if the leading packet */
    if (!network_queue_peek_string(con->recv_queue_raw, NET_HEADER_SIZE, &header)) {
        /* too small */

        return NETWORK_SOCKET_WAIT_FOR_EVENT;
    }

    packet_len = network_mysqld_proto_get_packet_len(&header);
    packet_id  = network_mysqld_proto_get_packet_id(&header);

    /* move the packet from the raw queue to the recv-queue */
    if ((packet = network_queue_pop_string(con->recv_queue_raw, packet_len + NET_HEADER_SIZE, NULL))) {
#ifdef NETWORK_DEBUG_TRACE_IO
        /* to trace the data we received from the socket, enable this */
        g_debug_hexdump(G_STRLOC, S(packet));
#endif

        if (con->packet_id_is_reset) {
            con->last_packet_id = packet_id;
            con->packet_id_is_reset = FALSE;
        } else if (packet_id != (guint8)(con->last_packet_id + 1)) {
            gchar *err_msg = g_strdup_printf("received packet-id %d from %s but expected %d",
                        packet_id, NETWORK_SOCKET_DST_NAME(con), con->last_packet_id + 1);
            g_log_dbproxy(g_warning, "%s", err_msg);
            g_free(err_msg);
            return NETWORK_SOCKET_ERROR;
        } else {
            con->last_packet_id = packet_id;
        }
    
        network_queue_append(con->recv_queue, packet);
    } else {
        return NETWORK_SOCKET_WAIT_FOR_EVENT;
    }

    return NETWORK_SOCKET_SUCCESS;
}

/**
 * read a MySQL packet from the socket
 *
 * the packet is added to the con->recv_queue and contains a full mysql packet
 * with packet-header and everything 
 */
network_socket_retval_t network_mysqld_read(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    switch (network_socket_read(con)) {
    case NETWORK_SOCKET_WAIT_FOR_EVENT:
        return NETWORK_SOCKET_WAIT_FOR_EVENT;
    case NETWORK_SOCKET_ERROR:
        return NETWORK_SOCKET_ERROR;
    case NETWORK_SOCKET_SUCCESS:
        break;
    case NETWORK_SOCKET_ERROR_RETRY:
        g_log_dbproxy(g_error, "sock->src:%s sock->dst:%s read data by sock return NETWORK_SOCKET_ERROR_RETRY which wasn't expected",
                    NETWORK_SOCKET_SRC_NAME(con), NETWORK_SOCKET_DST_NAME(con));
        break;
    }

    return network_mysqld_con_get_packet(chas, con);
}

network_socket_retval_t network_mysqld_write(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    network_socket_retval_t ret;

    ret = network_socket_write(con, -1);

    return ret;
}

/**
 * call the hooks of the plugins for each state
 *
 * if the plugin doesn't implement a hook, we provide a default operation
 *
 * @param srv      the global context
 * @param con      the connection context
 * @param state    state to handle
 * @return         NETWORK_SOCKET_SUCCESS on success
 */
network_socket_retval_t plugin_call(chassis *srv, network_mysqld_con *con, int state) {
    network_socket_retval_t ret;
    NETWORK_MYSQLD_PLUGIN_FUNC(func) = NULL;

    switch (state) {
    case CON_STATE_INIT:
        func = con->plugins.con_init;

        if (!func) { /* default implementation */
            con->state = CON_STATE_CONNECT_SERVER;
        }
        break;
    case CON_STATE_CONNECT_SERVER:
        func = con->plugins.con_connect_server;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_HANDSHAKE;
        }

        break;
    case CON_STATE_SEND_HANDSHAKE:
        func = con->plugins.con_send_handshake;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_AUTH;
        }

        break;
    case CON_STATE_READ_HANDSHAKE:
        func = con->plugins.con_read_handshake;

        break;
    case CON_STATE_READ_AUTH:
        func = con->plugins.con_read_auth;

        break;
    case CON_STATE_SEND_AUTH:
        func = con->plugins.con_send_auth;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_AUTH_RESULT;
        }
        break;
    case CON_STATE_READ_AUTH_RESULT:
        func = con->plugins.con_read_auth_result;
        break;
    case CON_STATE_SEND_AUTH_RESULT:
        func = con->plugins.con_send_auth_result;

        if (!func) { /* default implementation */
            switch (con->auth_result_state) {
            case MYSQLD_PACKET_OK:
                con->state = CON_STATE_READ_QUERY;
                break;
            case MYSQLD_PACKET_ERR:
                CON_MSG_HANDLE(g_warning, con,
                        "the return status of auth result is ERR");
                con->state = CON_STATE_ERROR;
                break;
            case MYSQLD_PACKET_EOF:
                /**
                 * the MySQL 4.0 hash in a MySQL 4.1+ connection
                 */
                con->state = CON_STATE_READ_AUTH_OLD_PASSWORD;
                break;
            default:
                {
                gchar *msg = g_strdup_printf("unexpected state for SEND_AUTH_RESULT: %02x",
                        con->auth_result_state);
                CON_MSG_HANDLE(g_error, con, msg);
                g_free(msg);
                }
                }
        }
        break;
    case CON_STATE_READ_AUTH_OLD_PASSWORD: {
        /** move the packet to the send queue */
        GString *packet;
        GList *chunk;
        network_socket *recv_sock, *send_sock;

        recv_sock = con->client;
        send_sock = con->server;

        if (NULL == con->server) {
            /**
             * we have to auth against same backend as we did before
             * but the user changed it
             */

            g_log_dbproxy(g_warning, "(lua) read-auth-old-password failed as backend_ndx got reset.");

            network_mysqld_con_send_error(con->client, C("(lua) read-auth-old-password failed as backend_ndx got reset."));
            SEND_ERR_MSG_HANDLE(g_warning, "(lua) read-auth-old-password failed as backend_ndx got reset.", con->client);
            con->state = CON_STATE_SEND_ERROR;
            break;
        }

        chunk = recv_sock->recv_queue->chunks->head;
        packet = chunk->data;

        /* we aren't finished yet */
        network_queue_append(send_sock->send_queue, packet);

        g_queue_delete_link(recv_sock->recv_queue->chunks, chunk);

        /**
         * send it out to the client 
         */
        con->state = CON_STATE_SEND_AUTH_OLD_PASSWORD;
        break; }
    case CON_STATE_SEND_AUTH_OLD_PASSWORD:
        /**
         * data is at the server, read the response next 
         */
        con->state = CON_STATE_READ_AUTH_RESULT;
        break;
    case CON_STATE_READ_QUERY:
        func = con->plugins.con_read_query;
        break;
    case CON_STATE_READ_QUERY_RESULT:
        func = con->plugins.con_read_query_result;
        break;
    case CON_STATE_SEND_QUERY_RESULT:
        func = con->plugins.con_send_query_result;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_QUERY;
        }
        break;

    case CON_STATE_SEND_LOCAL_INFILE_DATA:
        func = con->plugins.con_send_local_infile_data;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_LOCAL_INFILE_RESULT;
        }

        break;
    case CON_STATE_READ_LOCAL_INFILE_DATA:
        func = con->plugins.con_read_local_infile_data;

        if (!func) { /* the plugins have to implement this function to track LOAD DATA LOCAL INFILE handling work */
            con->state = CON_STATE_ERROR;
        }

        break;
    case CON_STATE_SEND_LOCAL_INFILE_RESULT:
        func = con->plugins.con_send_local_infile_result;

        if (!func) { /* default implementation */
            con->state = CON_STATE_READ_QUERY;
        }

        break;
    case CON_STATE_READ_LOCAL_INFILE_RESULT:
        func = con->plugins.con_read_local_infile_result;

        if (!func) { /* the plugins have to implement this function to track LOAD DATA LOCAL INFILE handling work */
            con->state = CON_STATE_ERROR;
        }

        break;
    case CON_STATE_ERROR:
        CON_MSG_HANDLE(g_debug, con, "not executing plugin function in state CON_STATE_ERROR");
        return NETWORK_SOCKET_SUCCESS;
    default:
        {
            gchar *msg = g_strdup_printf("unhandled con state: %d", state);
            CON_MSG_HANDLE(g_error, con, msg);
            g_free(msg);
        }
    }
    if (!func) return NETWORK_SOCKET_SUCCESS;

//  LOCK_LUA(srv->priv->sc);    /*remove lock*/
    ret = (*func)(srv, con);
//  UNLOCK_LUA(srv->priv->sc);  /*remove lock*/

    return ret;
}

/**
 * reset the command-response parsing
 *
 * some commands needs state information and we have to 
 * reset the parsing as soon as we add a new command to the send-queue
 */
void network_mysqld_con_reset_command_response_state(network_mysqld_con *con) {
    con->parse.command = -1;
    if (con->parse.data && con->parse.data_free) {
        con->parse.data_free(con->parse.data);

        con->parse.data = NULL;
        con->parse.data_free = NULL;
    }
}

/**
 * get the name of a connection state
 */
const char *network_mysqld_con_state_get_name(network_mysqld_con_state_t state) {
    switch (state) {
    case CON_STATE_INIT: return "CON_STATE_INIT";
    case CON_STATE_CONNECT_SERVER: return "CON_STATE_CONNECT_SERVER";
    case CON_STATE_READ_HANDSHAKE: return "CON_STATE_READ_HANDSHAKE";
    case CON_STATE_SEND_HANDSHAKE: return "CON_STATE_SEND_HANDSHAKE";
    case CON_STATE_READ_AUTH: return "CON_STATE_READ_AUTH";
    case CON_STATE_SEND_AUTH: return "CON_STATE_SEND_AUTH";
    case CON_STATE_READ_AUTH_OLD_PASSWORD: return "CON_STATE_READ_AUTH_OLD_PASSWORD";
    case CON_STATE_SEND_AUTH_OLD_PASSWORD: return "CON_STATE_SEND_AUTH_OLD_PASSWORD";
    case CON_STATE_READ_AUTH_RESULT: return "CON_STATE_READ_AUTH_RESULT";
    case CON_STATE_SEND_AUTH_RESULT: return "CON_STATE_SEND_AUTH_RESULT";
    case CON_STATE_READ_QUERY: return "CON_STATE_READ_QUERY";
    case CON_STATE_SEND_QUERY: return "CON_STATE_SEND_QUERY";
    case CON_STATE_READ_QUERY_RESULT: return "CON_STATE_READ_QUERY_RESULT";
    case CON_STATE_SEND_QUERY_RESULT: return "CON_STATE_SEND_QUERY_RESULT";
    case CON_STATE_READ_LOCAL_INFILE_DATA: return "CON_STATE_READ_LOCAL_INFILE_DATA";
    case CON_STATE_SEND_LOCAL_INFILE_DATA: return "CON_STATE_SEND_LOCAL_INFILE_DATA";
    case CON_STATE_READ_LOCAL_INFILE_RESULT: return "CON_STATE_READ_LOCAL_INFILE_RESULT";
    case CON_STATE_SEND_LOCAL_INFILE_RESULT: return "CON_STATE_SEND_LOCAL_INFILE_RESULT";
    case CON_STATE_CLOSE_CLIENT: return "CON_STATE_CLOSE_CLIENT";
    case CON_STATE_CLOSE_SERVER: return "CON_STATE_CLOSE_SERVER";
    case CON_STATE_ERROR: return "CON_STATE_ERROR";
    case CON_STATE_SEND_ERROR: return "CON_STATE_SEND_ERROR";
    }

    return "unknown";
}

/**
 * handle the different states of the MySQL protocol
 *
 * @param event_fd     fd on which the event was fired
 * @param events       the event that was fired
 * @param user_data    the connection handle
 */
void network_mysqld_con_handle(int event_fd, short events, void *user_data) {
    network_mysqld_con_state_t ostate;
    network_mysqld_con *con = user_data;
    chassis *srv = con->srv;
    guint cur_threadid = chassis_event_get_threadid();
    int retval;
    network_socket_retval_t call_ret;
    network_mysqld_con_lua_t *st = NULL;
    gboolean will_exit = FALSE;
    gint64 timediff, timediff_us;

    g_assert(srv);
    g_assert(con);

    st = con->plugin_con_state;

    if (g_atomic_int_get(&con->conn_status.exit_phase) != CON_ALIVE_NORMAL) {
            will_exit = TRUE;
    }
    /*if (NULL != st && NULL != st->backend && IS_BACKEND_WAITING_EXIT(st->backend)) {
        will_exit = TRUE;
    }*/

    if (TRACE_WAIT_EVENT(con->srv->log->log_trace_modules))
    {
        GString *event_msg = g_string_new(NULL);
        gchar *event_obj = NULL;
        if (con->client && event_fd == con->client->fd) {
            event_obj = "client";
        } else if (con->server && event_fd == con->server->fd) {
            event_obj = "backend";
        }
        if (event_obj != NULL) {
            g_string_append_printf(event_msg, "%s event(type:%d,fd:%d) was waked up",
                                                    event_obj, events, event_fd);
            CON_MSG_HANDLE(g_message, con, event_msg->str);
        }
        g_string_free(event_msg, TRUE);
    }

    if (events == EV_READ) {
        int b = -1;

        /**
         * check how much data there is to read
         *
         * ioctl()
         * - returns 0 if connection is closed
         * - or -1 and ECONNRESET on solaris
         *   or -1 and EPIPE on HP/UX
         */
        if (ioctl(event_fd, FIONREAD, &b)) {
            switch (errno) {
            case E_NET_CONNRESET: /* solaris */
            case EPIPE: /* hp/ux */
                if (!con->is_in_wait && con->client && event_fd == con->client->fd) {
                    /* the client closed the connection, let's keep the server side open */
                    con->state = CON_STATE_CLOSE_CLIENT;
                } else if (con->server && event_fd == con->server->fd) {
                    con->state = CON_STATE_CLOSE_SERVER;
                } else {
                    /* server side closed on use, oops, close both sides */
                    con->state = CON_STATE_ERROR;
                }
                break;
            default:
                if (con->client && event_fd == con->client->fd) {
                    g_log_dbproxy(g_warning, "ioctl(%d, FIONREAD, ...) from Client(%s) failed: %s(%d)",
                                event_fd, NETWORK_SOCKET_SRC_NAME(con->client),
                                g_strerror(errno), errno);
                } else if (con->server && event_fd == con->server->fd) {
                    g_log_dbproxy(g_warning, "ioctl(%d, FIONREAD, ...) from Server:%s(thread_id:%u) failed: %s(%d)",
                                event_fd,
                                NETWORK_SOCKET_DST_NAME(con->server),
                                NETWORK_SOCKET_THREADID(con->server),
                                g_strerror(errno), errno);
                }
                con->state = CON_STATE_ERROR;
                break;
            }

                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
        } else if (b != 0) {
            if (con->client && event_fd == con->client->fd) {
                if (!con->is_in_wait) {
                    con->client->to_read = b;
                } else {
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    con->state = CON_STATE_ERROR;
                }
            } else if (con->server && event_fd == con->server->fd) {
                con->server->to_read = b;
            } else {
                g_log_dbproxy(g_error, "unexpected situation ioctl get data from neither client nor backend");
            }
        } else if (!will_exit) { /* Linux */
            /*  no data, may be in idle transaction during exit process. */
            if (con->client && event_fd == con->client->fd) {
                if (con->com_quit_seen) {
                    if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                        CON_MSG_HANDLE(g_message, con, "close both sides because of client will exit actively.");
                    }
                    g_atomic_int_add(&srv->proxy_closed_clients, 1);
                } else {
                    /* the client closed the connection, let's keep the server side open */
                    gchar *msg = "close both sides because of client was closed unexpectedly.";
                    if (con->state <= CON_STATE_READ_AUTH) {
                        //change log level from g_message to g_debug due to checking proxy status by MGW periodically.
                        CON_MSG_HANDLE(g_debug, con, msg);
                    } else {
                        CON_MSG_HANDLE(g_critical, con, msg);
                    }
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                }
                con->state = CON_STATE_CLOSE_CLIENT;
            } else if (con->server && event_fd == con->server->fd) {
                CON_MSG_HANDLE(g_warning, con, "close both sides because of server has closed the connection.");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_CLOSE_SERVER;
            } else {
                /* server side closed on use, oops, close both sides */
                CON_MSG_HANDLE(g_warning, con, "close both sides because of unknown reasons");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
            }
        }
    }

#define WAIT_FOR_EVENT(ev_struct, ev_type, timeout_s, timeout_us)                                 \
    timediff = timeout_s;                                                                         \
    timediff_us = timeout_us;                                                                     \
    event_set(&(ev_struct->event), ev_struct->fd, ev_type, network_mysqld_con_handle, user_data); \
    if (g_atomic_int_get(&con->conn_status.exit_phase) == CON_EXIT_TX) {                          \
        timediff = g_atomic_int_get(&srv->shutdown_timeout) -                                     \
                    (time(NULL) - g_atomic_int_get(&con->conn_status.exit_begin_time));           \
        timediff_us = 0;                                                                          \
    }                                                                                             \
    if (timediff < 0) {  timediff = 0; }                                                          \
    chassis_event_add_self(srv, &(ev_struct->event), timediff, timediff_us);                      \
    if (ev_struct == con->server) { \
        con->wait_status = (ev_type == EV_READ) ? CON_WAIT_SERVER_READ : CON_WAIT_SERVER_WRITE; \
    } else { \
        con->wait_status = (ev_type == EV_READ) ? CON_WAIT_CLIENT_READ : CON_WAIT_CLIENT_WRITE; \
    }                                                                                             \
    if (TRACE_WAIT_EVENT(con->srv->log->log_trace_modules))                                       \
    {                                                                                             \
        EMIT_WAIT_FOR_EVENT(ev_struct, ev_type, con);                                         \
    }

    /**
     * loop on the same connection as long as we don't end up in a stable state
     */

    if (event_fd != -1) {
        NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::done");
        con->wait_status = CON_NO_WAIT;
    } else {
        NETWORK_MYSQLD_CON_TRACK_TIME(con, "con_handle_begin");
    }

    if (con->client && event_fd == con->client->fd && con->com_quit_seen) {
        if (con->state != CON_STATE_CLOSE_CLIENT) {
            con->state = CON_STATE_CLOSE_CLIENT;
            if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                CON_MSG_HANDLE(g_message, con, "close both sides because of client will exit actively.");
            }
        }
    }

    do {
        ostate = con->state;
#ifdef NETWORK_DEBUG_TRACE_STATE_CHANGES
        /* if you need the state-change information without dtrace, enable this */
        g_log_dbproxy(g_debug, "[%d] %s",
                getpid(),
                network_mysqld_con_state_get_name(con->state));
#endif

        if (cur_threadid != 0) {
            gboolean is_con_exit = FALSE;
            gchar *err_msg = NULL;

            if (chassis_is_shutdown_normal() && con->state < CON_STATE_READ_QUERY) {
                is_con_exit = TRUE;
                err_msg = "closing connection immediately during connecting when shutdown normal";
            } else if (g_atomic_int_get(&con->conn_status.exit_phase) == CON_EXIT_TX) {
                gint64 time_diff = time(NULL) - g_atomic_int_get(&con->conn_status.exit_begin_time);

                if (time_diff >= g_atomic_int_get(&srv->shutdown_timeout)) {
                    is_con_exit = TRUE;
                    err_msg = "closing connection immediately if timeout expired when shutdown normal";
                } else if (con->state == CON_STATE_READ_QUERY &&
                                    !con->conn_status.is_in_transaction) {
                    is_con_exit = TRUE;
                    err_msg = "closing connection immediately if no active transactions when shutdown normal";
                }
            } else if (g_atomic_int_get(&con->conn_status.exit_phase) == CON_EXIT_KILL) {
                    is_con_exit = TRUE;
                    err_msg = "closing connection immediatly during shutdown immediate process or kill session";
            } 
            /*else if (st != NULL && st->backend != NULL && IS_BACKEND_WAITING_EXIT(st->backend)) {
                    gint64 time_diff = 0;

                    g_rw_lock_reader_lock(&st->backend->backend_lock);
                    time_diff = time(NULL) - st->backend->state_since - st->backend->offline_timeout;
                    g_rw_lock_reader_unlock(&st->backend->backend_lock);

                    if (time_diff >= 0) {
                        is_con_exit = TRUE;
                        err_msg = "closing connection immediately if timeout expired when offlining/removing";
                    } else if (con->state == CON_STATE_READ_QUERY &&
                                !con->conn_status.is_in_transaction) {
                        is_con_exit = TRUE;
                        err_msg = "closing connection immediately if no active transactions when offlining/removing";
                    }
             }
             */

            if (is_con_exit) {
                CON_MSG_HANDLE(g_warning, con, err_msg);
                con->state = CON_STATE_ERROR;
            }
        }

        MYSQLPROXY_STATE_CHANGE(event_fd, events, con->state);
        switch (con->state) {
        case CON_STATE_ERROR:
            /* we can't go on, close the connection */
            plugin_call_cleanup(srv, con);

            chassis_event_remove_connection(srv, con);

            network_mysqld_con_free(con);

            chassis_dec_connection(srv);

            con = NULL;

            return;
        case CON_STATE_CLOSE_CLIENT:
        case CON_STATE_CLOSE_SERVER:
            /* FIXME: this comment has nothing to do with reality...
             * the server connection is still fine, 
             * let's keep it open for reuse */

            plugin_call_cleanup(srv, con);

            chassis_event_remove_connection(srv, con);

            network_mysqld_con_free(con);

            chassis_dec_connection(srv);

            con = NULL;

            return;
        case CON_STATE_INIT:
            /* if we are a proxy ask the remote server for the hand-shake packet 
             * if not, we generate one */
            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                /**
                 * no luck, let's close the connection
                 */
                CON_MSG_HANDLE(g_critical, con, "con state init failed");
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                
                break;
            }

            break;
        case CON_STATE_CONNECT_SERVER:
            if (events == EV_TIMEOUT) {
                network_mysqld_con_lua_t *st = con->plugin_con_state;
                /** Currently Setting backend to DOWN&UP is only allowed by check_sate due to lock issue.
                 *if (!IS_BACKEND_OFFLINE(st->backend) &&
                 *       !IS_BACKEND_WAITING_EXIT(st->backend)) {
                 *   SET_BACKEND_STATE(st->backend, BACKEND_STATE_DOWN);
                 *   g_log_dbproxy(g_warning, "set backend (%s) state to DOWN", st->backend->addr->name->str);
                 *}
                 */
                if (st != NULL && st->backend != NULL) {
                    // Currently don't know the case in which this code would be executed, print log.
                    g_log_dbproxy(g_critical, "don't know the case in which this code would be executed: %d",
                                                             g_atomic_int_get(&st->backend->connected_clients));
                    //g_atomic_int_dec_and_test(&st->backend->connected_clients);
                    st->backend = NULL;
                    st->backend_ndx = -1;
                }
                network_socket_free(con->server);
                con->server = NULL;
                ostate = CON_STATE_INIT;

                break;
            }
            switch ((retval = plugin_call(srv, con, con->state))) {
            case NETWORK_SOCKET_SUCCESS:

                /**
                 * hmm, if this is success and we have something in the clients send-queue
                 * we just send it out ... who needs a server ? */

                if ((con->client != NULL && con->client->send_queue->chunks->length > 0) && 
                     con->server == NULL) {
                    /* we want to send something to the client */

                    con->state = CON_STATE_SEND_HANDSHAKE;
                } else {
                    g_assert(con->server);
                }

                break;
            case NETWORK_SOCKET_ERROR_RETRY:
                if (con->server) {
                    /**
                     * we have a server connection waiting to begin writable
                     */
                    WAIT_FOR_EVENT(con->server, EV_WRITE, 5, 0);
                    NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::connect_server");
                    return;
                } else {
                    /* try to get a connection to another backend,
                     *
                     * setting ostate = CON_STATE_INIT is a hack to make sure
                     * the loop is coming back to this function again */
                   ostate = CON_STATE_INIT;
                }

                break;
            case NETWORK_SOCKET_ERROR:
             {
                /**
                 * connecting failed and no option to retry
                 *
                 * close the connection
                 */
                    gchar *msg = "connect to dbproxy failed";

                    if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                        CON_MSG_HANDLE(g_warning, con, msg);
                    } else {
                        CON_MSG_HANDLE(g_warning, con, msg);
                    }

                    g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                    con->state = CON_STATE_SEND_ERROR;
                    break;
            }
            default:
                {
                    g_log_dbproxy(g_warning, "hook for CON_STATE_CONNECT_SERVER return invalid return code: %d", retval);
                    SEND_INTERNAL_ERR("hook for connect dbproxy return invalid value");
                    con->state = CON_STATE_SEND_ERROR;

                    g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                    break;
                }
            }

            break;
        case CON_STATE_READ_HANDSHAKE: {
            /**
             * read auth data from the remote mysql-server 
             */
            network_socket *recv_sock;
            recv_sock = con->server;
            g_assert(events == 0 || event_fd == recv_sock->fd);

            switch (network_mysqld_read(srv, recv_sock)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                /* call us again when you have a event */
                WAIT_FOR_EVENT(con->server, EV_READ, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_handshake");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_warning, con, "read handshake from backend failed");
                //if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                //    CON_MSG_HANDLE(g_message, con, "read handshake from backend failed");
                //}

                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_ERROR:
                /**
                 * we couldn't understand the pack from the server 
                 * 
                 * we have something in the queue and will send it to the client
                 * and close the connection afterwards
                 */
                CON_MSG_HANDLE(g_warning, con, "return ERROR when processing handshake from backend");
                //if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                //    CON_MSG_HANDLE(g_message, con, "return ERROR when processing handshake from backend");
                //}
                con->state = CON_STATE_SEND_ERROR;
                break;
            default:
                CON_MSG_HANDLE(g_message, con, "proxy read handshak return invalid value(neither SUCCESS nor ERROR)");
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }
    
            break; }
        case CON_STATE_SEND_HANDSHAKE: 
            /* send the hand-shake to the client and wait for a response */
            if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                CON_MSG_HANDLE(g_message, con, "send handshake");
            }

            switch (network_mysqld_write(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_handshake");
                
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /**
                 * writing failed, closing connection
                 */
                if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                    CON_MSG_HANDLE(g_warning, con, "send handshake to client failed");
                } else {
                    //change log level from g_warning to g_debug due to checking proxy status by MGW periodically.
                    CON_MSG_HANDLE(g_debug, con, "send handshake to client failed");
                }
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                    CON_MSG_HANDLE(g_message, con, "hook for sending handshake return invalid value");
                }
                SEND_INTERNAL_ERR("hook for sending handshake return invalid value");
                con->state = CON_STATE_SEND_ERROR;

                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                break;
            }

            break;
        case CON_STATE_READ_AUTH: {
            /* read auth from client */
            network_socket *recv_sock;

            recv_sock = con->client;

            g_assert(events == 0 || event_fd == recv_sock->fd);

            if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                CON_MSG_HANDLE(g_message, con, "to read auth packet");
            }

            switch (network_mysqld_read(srv, recv_sock)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_READ, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                {
                    gchar *msg = "to read auth packet from client failed";
                    if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                        CON_MSG_HANDLE(g_warning, con, msg);
                    } else {
                        CON_MSG_HANDLE(g_warning, con, msg);
                    }
                    con->state = CON_STATE_ERROR;

                    g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                    break;
                }
            }
            
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_ERROR:
                /* proxy_read_auth will emits some error log. */
                CON_MSG_HANDLE(g_warning, con, "process auth packet failed");
                con->state = CON_STATE_SEND_ERROR;

                break;
            default:
                {
                     gchar *msg = "hook for processing read auth packet return invalid value";
                     CON_MSG_HANDLE(g_warning, con, msg);
                     SEND_INTERNAL_ERR(msg);
                     con->state = CON_STATE_SEND_ERROR;

                     g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                     break;
                }
            }

            break; }
        case CON_STATE_SEND_AUTH:
            /* send the auth-response to the server */
            switch (network_mysqld_write(srv, con->server)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->server, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /* might be a connection close, we should just close the connection and be happy */
                CON_MSG_HANDLE(g_warning, con, "send auth to backend failed");
                con->state = CON_STATE_ERROR;
                break;
            }
            
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "change to CON_STATE_READ_AUTH_RESULT "
                                "from CON_STATE_SEND_AUTH failed");
                con->state = CON_STATE_ERROR;
                break;
            }

            break;
        case CON_STATE_READ_AUTH_RESULT: {
            /* read the auth result from the server */
            network_socket *recv_sock;
            GList *chunk;
            GString *packet;
            recv_sock = con->server;

            g_assert(events == 0 || event_fd == recv_sock->fd);

            switch (network_mysqld_read(srv, recv_sock)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->server, EV_READ, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth_result");
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_critical, con, "read auth result from backend failed");
                con->state = CON_STATE_ERROR;
                break;
            }
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            /**
             * depending on the result-set we have different exit-points
             * - OK  -> READ_QUERY
             * - EOF -> (read old password hash) 
             * - ERR -> ERROR
             */
            chunk = recv_sock->recv_queue->chunks->head;
            packet = chunk->data;
            g_assert(packet);
            g_assert(packet->len > NET_HEADER_SIZE);

            con->auth_result_state = packet->str[NET_HEADER_SIZE];

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "proxy_read_auth_result's returns invalid value");
                con->state = CON_STATE_ERROR;
                break;
            }

            break; }
        case CON_STATE_SEND_AUTH_RESULT: {
            /* send the hand-shake to the client and wait for a response */

            if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                CON_MSG_HANDLE(g_message, con, "send auth to client");
            }

            switch (network_mysqld_write(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth_result");
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_critical, con, "send auth result to client failed");
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }
            
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                {
                    if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                        CON_MSG_HANDLE(g_message, con, "[connect finished]");
                    }

                    break;
                }
            default:
                {
                    gchar *msg = "return invalid value after CON_STATE_SEND_AUTH_RESULT";
                    CON_MSG_HANDLE(g_critical, con, msg);
                    con->state = CON_STATE_ERROR;

                    g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                    break;
                }
            }

            break;
            }
        case CON_STATE_READ_AUTH_OLD_PASSWORD: 
            /* read auth from client */
            switch (network_mysqld_read(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_READ, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth_old_password");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_warning, con, "read auth old_password from client failed");

                //if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                //    CON_MSG_HANDLE(g_message, con, "read auth old_password from client failed");
                //}

                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }
            
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "return invalid value after CON_STATE_READ_AUTH_OLD_PASSWORD");
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            break; 
        case CON_STATE_SEND_AUTH_OLD_PASSWORD:
            /* send the auth-response to the server */
            switch (network_mysqld_write(srv, con->server)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->server, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth_old_password");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /* might be a connection close, we should just close the connection and be happy */
                CON_MSG_HANDLE(g_warning, con, "send auth old password to backend failed");
                //if (TRACE_CON_STATUS(con->srv->log->log_trace_modules)) {
                //    CON_MSG_HANDLE(g_message, con, "send auth old password to backend failed");
                //}
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "return invalid value after CON_STATE_SEND_AUTH_OLD_PASSWORD");
                g_atomic_int_add(&srv->proxy_aborted_connects, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            break;

        case CON_STATE_READ_QUERY: {
            network_socket *recv_sock = con->client;

            if (events == EV_TIMEOUT) {
                gchar *log_str = g_strdup_printf("close the noninteractive connection(%s) now because of timeout.",
                                                NETWORK_SOCKET_SRC_NAME(recv_sock));
                CON_MSG_HANDLE(g_critical, con, log_str);
                g_free(log_str);
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            // 异常，server端收到了数据，直接断开连接
            if (con->server && event_fd == con->server->fd) {
                gchar *log_str = g_strdup_printf("there is something to read from server(%s).",
                                                NETWORK_SOCKET_SRC_NAME(con->server));
                CON_MSG_HANDLE(g_critical, con, log_str);
                g_free(log_str);
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            g_assert(events == 0 || event_fd == recv_sock->fd);

            // 在之前的等待中，server event可能被加到event set中
            if (con->server) {
                event_del(&con->server->event);
            }

            network_packet last_packet;

            do {
                switch (network_mysqld_read(srv, recv_sock)) {
                case NETWORK_SOCKET_SUCCESS:
                    break;
                case NETWORK_SOCKET_WAIT_FOR_EVENT:
                    WAIT_FOR_EVENT(con->client, EV_READ, g_atomic_int_get(&srv->wait_timeout), 0);
                    if (con->server != NULL) {
                        event_set(&(con->server->event), con->server->fd, EV_READ, network_mysqld_con_handle, con);
                        chassis_event_add_self(con->srv, &(con->server->event), 0, 0); /* add a event, but stay in the same thread */
                    }
                    NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query");
                    return;
                case NETWORK_SOCKET_ERROR_RETRY:
                case NETWORK_SOCKET_ERROR:
                    CON_MSG_HANDLE(g_critical, con, "read query from client failed");
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    con->state = CON_STATE_ERROR;
                    break;
                }

                if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                last_packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
            } while (last_packet.data->len == PACKET_LEN_MAX + NET_HEADER_SIZE); /* read all chunks of the overlong data */

            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                if (con->state != CON_STATE_ERROR) {
                    GString* packets = g_string_new("read query:");
                    GString* packet = NULL;
                    int i;
                    for (i = 0; NULL != (packet = g_queue_peek_nth(recv_sock->recv_queue->chunks, i)); i++) {
                        g_string_append_len(packets, packet->str + NET_HEADER_SIZE, packet->len - NET_HEADER_SIZE);
                    }
                    CON_MSG_HANDLE(g_message, con, packets->str);
                    g_string_free(packets, TRUE);
                }
            }

            switch (plugin_call(srv, con, con->state)) {
                case NETWORK_SOCKET_SUCCESS:
                    break;
                case NETWORK_SOCKET_ERROR:
                    {
                        /* proxy_read_query emit error log. */
                        con->state = CON_STATE_SEND_ERROR;
                        break;
                    }
                default:
                    {
                        gchar *msg = "hook for processing read query packet return invalid value";
                        CON_MSG_HANDLE(g_critical, con, msg);
                        SEND_INTERNAL_ERR(msg);
                        con->state = CON_STATE_SEND_ERROR;

                        g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                        break;
                    }
                }

            /**
             * there should be 3 possible next states from here:
             *
             * - CON_STATE_ERROR (if something went wrong and we want to close the connection
             * - CON_STATE_SEND_QUERY (if we want to send data to the con->server)
             * - CON_STATE_SEND_QUERY_RESULT (if we want to send data to the con->client)
             *
             * @todo verify this with a clean switch ()
             */

            /* reset the tracked command
             *
             * if the plugin decided to send a result, it has to track the commands itself
             * otherwise LOAD DATA LOCAL INFILE and friends will fail
             */
            if (con->state == CON_STATE_SEND_QUERY) {
                network_mysqld_con_reset_command_response_state(con);
                if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                    network_mysqld_con_lua_t *st = con->plugin_con_state;
                    gchar *msg = NULL;
                    if (st->injected.queries->length > 0) {
                        injection *inj = g_queue_peek_head(st->injected.queries);
                        msg = g_strdup_printf("read query to send query  %s:%s", GET_COM_STRING(inj->query));
                    } else {
                        msg = g_strdup_printf("read query to send query"); 
                    }
                    CON_MSG_HANDLE(g_message, con, msg);
                    g_free(msg);
                }
            }

            break; }
        case CON_STATE_SEND_QUERY:
            /* send the query to the server
             *
             * this state will loop until all the packets from the send-queue are flushed 
             */

            if (events != EV_TIMEOUT && con->server->send_queue->offset == 0) {
                /* only parse the packets once */
                network_packet packet;

                packet.data = g_queue_peek_head(con->server->send_queue->chunks);
                packet.offset = 0;

                if (0 != network_mysqld_con_command_states_init(con, &packet)) {
                    SEND_INTERNAL_ERR("command state init failed before send query");
                    con->state = CON_STATE_SEND_ERROR;

                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    break;
                }
            }

            /* check status */
            if (con->srv->max_backend_tr > 0) {
                if (con->try_send_query_times < 2) {
                network_mysqld_con_lua_t *st = con->plugin_con_state;

                    if (IS_BACKEND_PENDING(st->backend)) {
                        gint cur_time = con->srv->thread_running_sleep_delay;
                    gint time_us = cur_time%MICROSEC;
                    gint time_s = cur_time/MICROSEC;

                    WAIT_FOR_EVENT(con->client, EV_READ, time_s, time_us);
                    con->is_in_wait = TRUE;
                        con->try_send_query_times++;
                    return;
                }
                } else if (con->try_send_query_times == 2) {
                    SEND_INTERNAL_ERR("current query cancelled due to sending query wait over 2 times");
                    con->state = CON_STATE_SEND_ERROR;
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    break;
                }
            }

            con->is_in_wait = FALSE;
            con->try_send_query_times = 0;

            switch (network_mysqld_write(srv, con->server)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->server, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_query");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                {
                    /* write() failed, close the connections. */
                    gchar *msg = "send query to backend failed";
                    CON_MSG_HANDLE(g_critical, con, msg);
                    SEND_INTERNAL_ERR(msg);
                    con->state = CON_STATE_SEND_ERROR;

                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    break;
                }
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            /* some statements don't have a server response */
            switch (con->parse.command) {
            case COM_STMT_SEND_LONG_DATA: /* not acked */
            case COM_STMT_CLOSE:
                network_mysqld_stat_stmt_end(con, chassis_get_rel_microseconds());
                con->state = CON_STATE_READ_QUERY;
                if (con->client) network_mysqld_queue_reset(con->client);
                if (con->server) network_mysqld_queue_reset(con->server);
                break;
            default:
                con->state = CON_STATE_READ_QUERY_RESULT;
                break;
            }

            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                if (con->state == CON_STATE_READ_QUERY_RESULT) {
                    CON_MSG_HANDLE(g_message, con, "send query to read query result");
                }
            }

            break; 
        case CON_STATE_READ_QUERY_RESULT: 
            /* read all packets of the resultset 
             *
             * depending on the backend we may forward the data to the client right away
             */
            do {
                network_socket *recv_sock;

                recv_sock = con->server;

                g_assert(events == 0 || event_fd == recv_sock->fd);

                switch (network_mysqld_read(srv, recv_sock)) {
                case NETWORK_SOCKET_SUCCESS:
                    break;
                case NETWORK_SOCKET_WAIT_FOR_EVENT:
                    WAIT_FOR_EVENT(con->server, EV_READ, 0, 0);
                    NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query_result");
                    return;
                case NETWORK_SOCKET_ERROR_RETRY:
                case NETWORK_SOCKET_ERROR:
                    {
                        gchar *msg = "read query result from backend failed";
                        CON_MSG_HANDLE(g_critical, con, msg);
                        SEND_INTERNAL_ERR(msg);
                        con->state = CON_STATE_SEND_ERROR;

                        g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                        break;
                    }
                }

                if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                    if (con->state != CON_STATE_ERROR && con->state != CON_STATE_SEND_ERROR) {
                        CON_MSG_HANDLE(g_message, con, "read one query result success");
                    }
                }

                switch (plugin_call(srv, con, con->state)) {
                case NETWORK_SOCKET_SUCCESS:
                    /* if we don't need the resultset, forward it to the client */
                    if (!con->resultset_is_finished && !con->resultset_is_needed) {
                        /* check how much data we have in the queue waiting, no need to try to send 5 bytes */
                        if (con->client->send_queue->len > 64 * 1024) {
                            con->state = CON_STATE_SEND_QUERY_RESULT;
                        }
                    }
                    break;
                case NETWORK_SOCKET_ERROR:
                    {
                        /* something nasty happend, let's close the connection */
                        gchar *msg = "proxy_read_query_result return ERROR";

                        CON_MSG_HANDLE(g_critical, con, msg);
                        SEND_INTERNAL_ERR(msg);
                        con->state = CON_STATE_SEND_ERROR;

                        g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    }
                    break;
                default:
                    {
                        gchar *msg = "hook for reading query result return invalid value";

                        CON_MSG_HANDLE(g_critical, con, msg);
                        SEND_INTERNAL_ERR(msg);
                        con->state = CON_STATE_SEND_ERROR;

                        g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                        break;
                    }
                }

                if (g_atomic_int_get(&con->conn_status.exit_phase) == CON_EXIT_KILL) {
                    break;
                }

                if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                    if (con->state == CON_STATE_READ_QUERY_RESULT) {
                        CON_MSG_HANDLE(g_message, con, "read next query result");
                    }
                }
            } while (con->state == CON_STATE_READ_QUERY_RESULT);

            if (g_atomic_int_get(&con->conn_status.exit_phase) == CON_EXIT_KILL) {
                con->state = CON_STATE_ERROR;
                g_log_dbproxy(g_warning, "connection close immediatly during shutdown immediate or kill session.");
            }

            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                if (con->state == CON_STATE_SEND_QUERY_RESULT) {
                    CON_MSG_HANDLE(g_message, con, "read query result to send query result");
                }
            }

            break; 
        case CON_STATE_SEND_QUERY_RESULT:
            /**
             * send the query result-set to the client */
            switch (network_mysqld_write(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_query_result");
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /**
                 * client is gone away
                 *
                 * close the connection and clean up
                 */
                CON_MSG_HANDLE(g_critical, con, "send query result to client failed");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                if (con->state != CON_STATE_ERROR && con->state != CON_STATE_SEND_ERROR) {
                    CON_MSG_HANDLE(g_message, con, "send query result success");
                }
            }

            /* if the write failed, don't call the plugin handlers */
            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            /* in case we havn't read the full resultset from the server yet, go back and read more
             */
            if (!con->resultset_is_finished && con->server) {
                con->state = CON_STATE_READ_QUERY_RESULT;
                if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                    CON_MSG_HANDLE(g_message, con, "send query result to read query result");
                }

                break;
            }

            switch (plugin_call(srv, con, con->state)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "proxy_send_query_result return not SUCCESS");
                SEND_INTERNAL_ERR("hook for sending query result return invalid value");
                con->state = CON_STATE_SEND_ERROR;

                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
            }

            /* special treatment for the LOAD DATA LOCAL INFILE command */
            if (con->state != CON_STATE_ERROR &&
                con->state != CON_STATE_SEND_ERROR &&
                con->parse.command == COM_QUERY &&
                1 == network_mysqld_com_query_result_is_local_infile(con->parse.data)) {
                con->state = CON_STATE_READ_LOCAL_INFILE_DATA;
            }

            if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                if (con->state == CON_STATE_SEND_QUERY) {
                    network_mysqld_con_lua_t *st = con->plugin_con_state;
                    injection *inj = g_queue_peek_head(st->injected.queries);
                    gchar *msg = g_strdup_printf("send query result to send query %s:%s",
                                                                    GET_COM_STRING(inj->query));
                    CON_MSG_HANDLE(g_message, con, msg);
                    g_free(msg);
                }
            }

            break;
        case CON_STATE_READ_LOCAL_INFILE_DATA: {
            /**
             * read the file content from the client 
             */
            network_socket *recv_sock;

            recv_sock = con->client;

            /**
             * LDLI is usually a whole set of packets
             */
            do {
                switch (network_mysqld_read(srv, recv_sock)) {
                case NETWORK_SOCKET_SUCCESS:
                    break;
                case NETWORK_SOCKET_WAIT_FOR_EVENT:
                    /* call us again when you have a event */
                    WAIT_FOR_EVENT(recv_sock, EV_READ, 0, 0);
                    NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_load_infile_data");

                    return;
                case NETWORK_SOCKET_ERROR_RETRY:
                case NETWORK_SOCKET_ERROR:
                    CON_MSG_HANDLE(g_critical, con, "read local infile data from client failed");
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    con->state = CON_STATE_ERROR;
                    break;
                }

                if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                switch ((call_ret = plugin_call(srv, con, con->state))) {
                case NETWORK_SOCKET_SUCCESS:
                    break;
                default:
                    CON_MSG_HANDLE(g_critical, con, "proxy_read_local_infile_data return not SUCCESS");
                    g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                    con->state = CON_STATE_ERROR;
                    break;
                }
            } while (con->state == ostate); /* read packets from the network until the plugin decodes to go to the next state */
    
            break; }
        case CON_STATE_SEND_LOCAL_INFILE_DATA: 
            /* send the hand-shake to the client and wait for a response */

            switch (network_mysqld_write(srv, con->server)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->server, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_load_infile_data");
                
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /**
                 * writing failed, closing connection
                 */
                CON_MSG_HANDLE(g_critical, con, "send local infile data failed");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch ((call_ret = plugin_call(srv, con, con->state))) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "return not SUCCESS after CON_STATE_SEND_LOCAL_INFILE_DATA");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            break;
        case CON_STATE_READ_LOCAL_INFILE_RESULT: {
            /**
             * read auth data from the remote mysql-server 
             */
            network_socket *recv_sock;
            recv_sock = con->server;
            g_assert(events == 0 || event_fd == recv_sock->fd);

            switch (network_mysqld_read(srv, recv_sock)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                /* call us again when you have a event */
                WAIT_FOR_EVENT(recv_sock, EV_READ, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_load_infile_result");

                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_critical, con, "read local infile result from backend failed");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch ((call_ret = plugin_call(srv, con, con->state))) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "return invalid value after CON_STATE_READ_LOCAL_INFILE_RESULT");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }
    
            break; }
        case CON_STATE_SEND_LOCAL_INFILE_RESULT: 
            /* send the hand-shake to the client and wait for a response */

            switch (network_mysqld_write(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_load_infile_result");
                
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                /**
                 * writing failed, closing connection
                 */
                CON_MSG_HANDLE(g_critical, con, "send local infile result to client failed");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

            switch ((call_ret = plugin_call(srv, con, con->state))) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            default:
                CON_MSG_HANDLE(g_critical, con, "return invalid value after CON_STATE_SEND_LOCAL_INFILE_RESULT");
                g_atomic_int_add(&srv->proxy_aborted_clients, 1);
                con->state = CON_STATE_ERROR;
                break;
            }

            break;
        case CON_STATE_SEND_ERROR:
            /**
             * send error to the client
             * and close the connections afterwards
             *  */
            switch (network_mysqld_write(srv, con->client)) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                WAIT_FOR_EVENT(con->client, EV_WRITE, 0, 0);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_error");
                return;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                CON_MSG_HANDLE(g_critical, con, "send error packet to client failed");
                con->state = CON_STATE_ERROR;
                break;
            }
            g_atomic_int_add(&srv->proxy_aborted_clients, 1);

            con->state = CON_STATE_ERROR;

            break;
        }

        event_fd = -1;
        events   = 0;
    } while (ostate != con->state);
    NETWORK_MYSQLD_CON_TRACK_TIME(con, "con_handle_end");

    return;
}

/**
 * accept a connection
 *
 * event handler for listening connections
 *
 * @param event_fd     fd on which the event was fired
 * @param events       the event that was fired
 * @param user_data    the listening connection handle
 * 
 */
void network_mysqld_con_accept(int G_GNUC_UNUSED event_fd, short events, void *user_data) {
    network_mysqld_con *listen_con = user_data;
    network_mysqld_con *client_con;
    network_socket *client;

    g_assert(events == EV_READ);
    g_assert(listen_con->server);

    client = network_socket_accept(listen_con->server);
    if (!client) return;

    /* looks like we open a client connection */
    client_con = network_mysqld_con_new();
    client_con->client = client;

    NETWORK_MYSQLD_CON_TRACK_TIME(client_con, "accept");

    network_mysqld_add_connection(listen_con->srv, client_con);
    network_socket_set_chassis(client, client_con->srv);

    if (TRACE_CON_STATUS(client_con->srv->log->log_trace_modules)) {
        CON_MSG_HANDLE(g_message, client_con, "new proxy connection accepted");
    }

    if (!chassis_add_connection(listen_con->srv))
    {
        network_mysqld_con_send_error_full_nolog(client, C("Proxy: Too many connections"), ER_CON_COUNT_ERROR, "08004");
        SEND_ERR_MSG_HANDLE(g_warning, "Proxy: Too many connections", client);
        client_con->state = CON_STATE_SEND_ERROR;
    }

    g_atomic_int_add(&client_con->srv->proxy_attempted_connects, 1);

    /**
     * inherit the config to the new connection 
     */

    client_con->plugins = listen_con->plugins;
//  client_con->config  = listen_con->config;

    //network_mysqld_con_handle(-1, 0, client_con);
    //�˴���client_con�����첽���У�Ȼ��ping�����̣߳��ɹ����߳�ȥִ��network_mysqld_con_handle�����������߳�ֱ��ִ��network_mysqld_con_handle
    chassis_event_add(client_con);
}

void network_mysqld_admin_con_accept(int G_GNUC_UNUSED event_fd, short events, void *user_data) {
    network_mysqld_con *listen_con = user_data;
    network_mysqld_con *client_con;
    network_socket *client;

    g_assert(events == EV_READ);
    g_assert(listen_con->server);

    client = network_socket_accept(listen_con->server);
    if (!client) return;


    /* looks like we open a client connection */
    client_con = network_mysqld_con_new();
    client_con->client = client;

    NETWORK_MYSQLD_CON_TRACK_TIME(client_con, "accept");

    network_mysqld_add_connection(listen_con->srv, client_con);

    if (TRACE_CON_STATUS(client_con->srv->log->log_trace_modules)) {
        CON_MSG_HANDLE(g_message, client_con, "new admin connection accepted");
    }

    if (!chassis_add_connection(listen_con->srv))
    {
        network_mysqld_con_send_error_full_nolog(client, C("Proxy: Too many connections"), ER_CON_COUNT_ERROR, "08004");
        SEND_ERR_MSG_HANDLE(g_warning, "Proxy: Too many connections", client);
        client_con->state = CON_STATE_SEND_ERROR;
    }

    /**
     * inherit the config to the new connection 
     */

    client_con->plugins = listen_con->plugins;
    client_con->config  = listen_con->config;

    chassis_event_add_connection(listen_con->srv, NULL, client_con);
    network_socket_set_chassis(client, client_con->srv);

    network_mysqld_con_handle(-1, 0, client_con);
}

/**
 * @todo move to network_mysqld_proto
 */
int network_mysqld_con_send_resultset(network_socket *con, GPtrArray *fields, GPtrArray *rows) {
    GString *s;
    gsize i, j;

    g_assert(fields->len > 0);

    s = g_string_new(NULL);

    /* - len = 99
     *  \1\0\0\1 
     *    \1 - one field
     *  \'\0\0\2 
     *    \3def 
     *    \0 
     *    \0 
     *    \0 
     *    \21@@version_comment 
     *    \0            - org-name
     *    \f            - filler
     *    \10\0         - charset
     *    \34\0\0\0     - length
     *    \375          - type 
     *    \1\0          - flags
     *    \37           - decimals
     *    \0\0          - filler 
     *  \5\0\0\3 
     *    \376\0\0\2\0
     *  \35\0\0\4
     *    \34MySQL Community Server (GPL)
     *  \5\0\0\5
     *    \376\0\0\2\0
     */

    network_mysqld_proto_append_lenenc_int(s, fields->len); /* the field-count */
    network_mysqld_queue_append(con, con->send_queue, S(s));

    for (i = 0; i < fields->len; i++) {
        MYSQL_FIELD *field = fields->pdata[i];
        
        g_string_truncate(s, 0);

        network_mysqld_proto_append_lenenc_string(s, field->catalog ? field->catalog : "def");   /* catalog */
        network_mysqld_proto_append_lenenc_string(s, field->db ? field->db : "");                /* database */
        network_mysqld_proto_append_lenenc_string(s, field->table ? field->table : "");          /* table */
        network_mysqld_proto_append_lenenc_string(s, field->org_table ? field->org_table : "");  /* org_table */
        network_mysqld_proto_append_lenenc_string(s, field->name ? field->name : "");            /* name */
        network_mysqld_proto_append_lenenc_string(s, field->org_name ? field->org_name : "");    /* org_name */

        g_string_append_c(s, '\x0c');                  /* length of the following block, 12 byte */
        g_string_append_c(s, field->charsetnr & 0xff);        /* charset */
        g_string_append_c(s, (field->charsetnr >> 8) & 0xff); /* charset */
        g_string_append_c(s, (field->length >> 0) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 8) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 16) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 24) & 0xff); /* len */
        g_string_append_c(s, field->type);             /* type */
        g_string_append_c(s, field->flags & 0xff);     /* flags */
        g_string_append_c(s, (field->flags >> 8) & 0xff); /* flags */
        g_string_append_c(s, 0);                       /* decimals */
        g_string_append_len(s, "\x00\x00", 2);         /* filler */
#if 0
        /* this is in the docs, but not on the network */
        network_mysqld_proto_append_lenenc_string(s, field->def);         /* default-value */
#endif
        network_mysqld_queue_append(con, con->send_queue, S(s));
    }

    g_string_truncate(s, 0);
    
    /* EOF */   
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */
    
    network_mysqld_queue_append(con, con->send_queue, S(s));

    for (i = 0; i < rows->len; i++) {
        GPtrArray *row = rows->pdata[i];

        g_string_truncate(s, 0);

        for (j = 0; j < row->len; j++) {
            network_mysqld_proto_append_lenenc_string(s, row->pdata[j]);
        }

        network_mysqld_queue_append(con, con->send_queue, S(s));
    }

    g_string_truncate(s, 0);

    /* EOF */   
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */

    network_mysqld_queue_append(con, con->send_queue, S(s));
    network_mysqld_queue_reset(con);

    g_string_free(s, TRUE);

    return 0;
}

void
network_mysqld_con_send_1_int_resultset(network_mysqld_con *con, gint info_type)
{
    MYSQL_FIELD     *field;
    GPtrArray       *rows = NULL, *row = NULL, *fields = NULL;
    gint i = 0;

    if (info_type >= INFO_FUNC_MAX || info_type < INFO_FUNC_FOUND_ROWS) { return ;}

    fields = network_mysqld_proto_fielddefs_new();

    /* column names */
    field = network_mysqld_proto_fielddef_new();
    field->name = g_strdup(con->conn_status.info_funcs[info_type].field_name->str);
    field->type = FIELD_TYPE_VAR_STRING;
    g_ptr_array_add(fields, field);

    /* row value */
    rows = g_ptr_array_new();
    row = g_ptr_array_new_with_free_func((GDestroyNotify)g_free);
    g_ptr_array_add(row, g_strdup_printf("%d",
                          con->conn_status.info_funcs[info_type].field_value));
    g_ptr_array_add(rows, row);

    network_mysqld_con_send_resultset(con->client, fields, rows);

    if (fields) {
        network_mysqld_proto_fielddefs_free(fields);
        fields = NULL;
    }

    if (rows) {
        for (i = 0; i < rows->len; i++) {
            g_ptr_array_free(row, TRUE);
        }
        g_ptr_array_free(rows, TRUE);
        rows = NULL;
    }

    if (con->conn_status.info_funcs[info_type].field_name->len > 0) {
        g_string_free(con->conn_status.info_funcs[info_type].field_name, TRUE);
        con->conn_status.info_funcs[info_type].field_name = g_string_new(NULL);
    }
}

gint64
network_mysqld_con_get_1_int_from_result_set(network_mysqld_con *con, void* inj_raw)
{
    proxy_resultset_t       *res = NULL;
    gint64                 v = 0;
    injection *inj = (injection *) inj_raw;

    if (!inj->resultset_is_needed ||
                !con->server->recv_queue->chunks ||
                inj->qstat.binary_encoded) {
        return 0;
    }

    res = proxy_resultset_new();
    res->result_queue = con->server->recv_queue->chunks;
    res->qstat = inj->qstat;
    res->rows  = inj->rows;
    res->bytes = inj->bytes;

    if (parse_resultset_fields(res) != 0) {
        if (TRACE_SHARD(con->srv->log->log_trace_modules)) {
            CON_MSG_HANDLE(g_warning, con,
                        "parse result fields failed during get 1 int value");
        }
        return 0;
    }

    GList *res_row = res->rows_chunk_head;
    while (res_row) {
        guint64 field_len;
        network_packet packet;

        packet.data = res_row->data;
        packet.offset = 0;

        network_mysqld_proto_skip_network_header(&packet);
        network_mysqld_lenenc_type lenenc_type;
        network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);

        switch (lenenc_type) {
            case NETWORK_MYSQLD_LENENC_TYPE_INT:
                {
                    gchar *value_str = NULL, *endptr = NULL;
 
                    /* get value length */
                    network_mysqld_proto_get_lenenc_int(&packet, &field_len);

                    /* get value content */
                    network_mysqld_proto_get_string_len(&packet, &value_str, field_len);

                    v = strtoll(value_str, &endptr, 0);
                    if ((endptr != NULL && *endptr != '\0') ||
                            (errno == ERANGE && (v == G_MAXINT64 || v == G_MININT64)) ||
                            endptr == value_str) {
                       v = 0;
                    }
                    g_free(value_str);
                }
                break;
           default:
               break;
       }
       res_row = res_row->next; /* only one row, next will exit loop */
    }

    proxy_resultset_free(res);

    return v;
}

void network_mysqld_query_stat(network_mysqld_con *con, char com_type, gboolean is_write)
{
    network_mysqld_stat_type_t qry_type = com_type == COM_QUERY ? (is_write ? THREAD_STAT_COM_QUERY_WRITE : THREAD_STAT_COM_QUERY_READ) : THREAD_STAT_COM_OTHER;
    thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);

    thread_status_var->thread_stat[qry_type]++;
}

void network_mysqld_send_query_stat(network_mysqld_con *con, char com_type, gboolean is_write)
{
    network_mysqld_stat_type_t qry_type =com_type == COM_QUERY ? (is_write ? THREAD_STAT_COM_QUERY_SEND_WRITE : THREAD_STAT_COM_QUERY_SEND_READ) : THREAD_STAT_COM_OTHER_SEND;
    thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);

    thread_status_var->thread_stat[qry_type]++;
}

void network_mysqld_socket_stat(chassis *chas, network_socket_dir_t socket_dir, gboolean is_write, guint len)
{
    thread_status_var_t *thread_status_var = chassis_event_thread_get_status(chas);

    if (socket_dir == SOCKET_SERVER) {
        if (is_write)
        {
            thread_status_var->thread_stat[THREAD_STAT_SERVER_WRITE] += len;
            thread_status_var->thread_stat[THREAD_STAT_SERVER_WRITE_PKT]++;
        } else {
            thread_status_var->thread_stat[THREAD_STAT_SERVER_READ] += len;
            thread_status_var->thread_stat[THREAD_STAT_SERVER_READ_PKT]++;
        }
    }
    else if (socket_dir == SOCKET_CLIENT) {
        if (is_write)
        {
            thread_status_var->thread_stat[THREAD_STAT_CLIENT_WRITE] += len;
            thread_status_var->thread_stat[THREAD_STAT_CLIENT_WRITE_PKT]++;
        } else {
            thread_status_var->thread_stat[THREAD_STAT_CLIENT_READ] += len;
            thread_status_var->thread_stat[THREAD_STAT_CLIENT_READ_PKT]++;
        }
    }
}

void network_mysqld_stat_stmt_start(network_mysqld_con *con, const char *cur_query, gint com_type)
{
    con->conn_status_var.query_running = TRUE;
    thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);
    g_atomic_pointer_add(&(thread_status_var->thread_stat[THREAD_STAT_THREADS_RUNNING]), 1);

    if (con->srv->query_response_time_stats > 0) {
        con->conn_status_var.cur_query_type = COM_OTHER;
    }

        con->conn_status_var.cur_query_start_time = chassis_get_rel_microseconds();
    if (cur_query != NULL) {
        memset(con->conn_status_var.cur_query, 0, STMT_LENTH);
        g_snprintf(con->conn_status_var.cur_query, STMT_LENTH, "%s", cur_query);
    }
    con->conn_status_var.cur_query_com_type = com_type;
}

void network_mysqld_stat_stmt_parser_end(network_mysqld_con *con, guint com_type, gboolean is_write)
{
    if (con->srv->query_response_time_stats > 0) {
        guint query_type = com_type == COM_QUERY ? (is_write ? COM_QUERY_WRITE : COM_QUERY_READ) : COM_OTHER;
        con->conn_status_var.cur_query_type = query_type;
    }

    network_mysqld_query_stat(con, com_type, is_write);
}

void network_mysqld_stat_stmt_end(network_mysqld_con *con, gint64 cur_time)
{
    guint respon_time = 0;
    guint query_type = con->conn_status_var.cur_query_type;

    if (con->conn_status_var.query_running && con->srv->query_response_time_stats > 0)
    {
        thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);

        respon_time = cur_time - con->conn_status_var.cur_query_start_time;

        thread_status_var->all_query_rt_stat[query_type].query_num++;
        thread_status_var->all_query_rt_stat[query_type].total_respon_time += respon_time;

        SET_MIN(thread_status_var->all_query_rt_stat[query_type].min_respon_time, respon_time);
        SET_MAX(thread_status_var->all_query_rt_stat[query_type].max_respon_time, respon_time);

        if (con->srv->long_query_time > 0 && respon_time > con->srv->long_query_time * 1000)
        {
            thread_status_var->thread_stat[THREAD_STAT_SLOW_QUERY]++;
            thread_status_var->thread_stat[THREAD_STAT_SLOW_QUERY_RT] += respon_time;
        }

        if (con->srv->query_response_time_stats == 2 && con->srv->query_response_time_range_base > 1)
        {
            guint hist_level = 0;
            for ( ; hist_level < MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS - 1; hist_level++)
            {
                if (respon_time <= pow(con->srv->query_response_time_range_base, hist_level) * 1000) {
                    break;
                }
            }

            if (hist_level < MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS)
            {
                thread_status_var->hist_query_rt_stat[query_type][hist_level].query_num++;
                thread_status_var->hist_query_rt_stat[query_type][hist_level].total_respon_time += respon_time;

                SET_MIN(thread_status_var->hist_query_rt_stat[query_type][hist_level].min_respon_time, respon_time);
                SET_MAX(thread_status_var->hist_query_rt_stat[query_type][hist_level].max_respon_time, respon_time);
            }
        }
    }

    con->conn_status_var.cur_query_start_time = cur_time;
    memset(con->conn_status_var.cur_query, 0, STMT_LENTH);
    con->conn_status_var.cur_query_com_type = 0;
    if(con->conn_status_var.query_running)
    {
        thread_status_var_t *thread_status_var = chassis_event_thread_get_status(con->srv);
        g_atomic_pointer_add(&(thread_status_var->thread_stat[THREAD_STAT_THREADS_RUNNING]), -1);
    }
    con->conn_status_var.query_running = FALSE;

    return ;
}

static gchar *
get_rotate_file_name(const gchar *log_name)
{
    gchar *rotatename = NULL;
    time_t t = time(NULL);
    struct tm cur_tm;

    g_assert(log_name != NULL);

    localtime_r(&t, &cur_tm);
    rotatename = g_strdup_printf("%s_%04d%02d%02d%02d%02d%02d",
                                log_name, cur_tm.tm_year + 1900,
                                cur_tm.tm_mon + 1, cur_tm.tm_mday, cur_tm.tm_hour,
                                cur_tm.tm_min, cur_tm.tm_sec);
    return rotatename;
}

static void
register_logfilenames(gchar *new_log_filename, gint max_file_num, GQueue **log_filenames_list)
{
    g_assert(new_log_filename != NULL);
    g_assert(max_file_num >= 0);

    if (*log_filenames_list == NULL) {
        *log_filenames_list = g_queue_new();
    } else {
        GList *tmp = g_queue_find_custom(*log_filenames_list, new_log_filename,
                                            (GCompareFunc)strcmp);
        if (tmp != NULL) {
            g_log_dbproxy(g_debug, "file %s is exist", new_log_filename);
            return ;
        }
    }
    g_queue_push_tail(*log_filenames_list, g_strdup(new_log_filename));

    /* add new file name to tail */
    if (g_queue_get_length(*log_filenames_list) > max_file_num)
    {
        gint i = g_queue_get_length(*log_filenames_list) - max_file_num;

        while (i)
        {
            gchar *rm_logfile = (gchar *)g_queue_pop_head(*log_filenames_list);
            if (unlink(rm_logfile) != 0)
            {
                g_log_dbproxy(g_warning, "rm log file %s failed", rm_logfile);
            } else {
                g_log_dbproxy(g_debug, "rm log file %s success %d %d %d", rm_logfile, i, max_file_num,
                            g_queue_get_length(*log_filenames_list));
            }
            g_free(rm_logfile);
            i--;
        }
    }

    return ;
}

gint
kill_one_connection(chassis *chas, guint64 kill_con_id)
{
    chassis_event_thread_t      *thread = NULL;
    network_mysqld_con          *conn = NULL;
    GList                       *gl_conn = NULL;
    gint                        ret = 1;
    guint32                     thread_id = GET_THEAD_ID(kill_con_id);

    if (thread_id < 0 || thread_id > chas->event_thread_count) {
        g_log_dbproxy(g_warning, "error thread id during kill connection");
        return ret;
    }

    if ((GET_CON_IDX(kill_con_id) < CON_PRE_FIRST_ID || GET_CON_IDX(kill_con_id) > CON_IDX_MASK)) {
        g_log_dbproxy(g_warning, "error connection id during kill connection");
        return ret;
    }

    thread = g_ptr_array_index(chas->threads, thread_id);

    g_rw_lock_reader_lock(&thread->connection_lock);
    gl_conn = thread->connection_list;
    while (gl_conn) {
        conn = gl_conn->data;
        if (conn->con_id == kill_con_id) {
            break;
        }

        gl_conn = g_list_next(gl_conn);
    }

    if (gl_conn != NULL) {
        g_assert(conn != NULL);

        g_atomic_int_set(&conn->conn_status.exit_phase, CON_EXIT_KILL);
        g_log_dbproxy(g_debug, "set connection %lu kill status.", kill_con_id);
        ret = 0;
    } else {
        g_log_dbproxy(g_debug, "connect id %lu in event thread %d no found.", kill_con_id, thread->index);
    }
    g_rw_lock_reader_unlock(&thread->connection_lock);

    return ret;
}

static info_func *
info_funcs_new(gint info_func_nums)
{
    gint i = 0;
    g_assert(info_func_nums > 0);

    info_func *info_funcs = (info_func *) g_new0(info_func, info_func_nums);

    for (i = INFO_FUNC_FOUND_ROWS; i < info_func_nums; i++) {
        info_funcs[i].info_func_id = i;
        info_funcs[i].field_name = g_string_new(NULL);
        info_funcs[i].field_value = 0;
    }

    return info_funcs;
}

void
info_funcs_free(info_func *info_funcs)
{
    gint i = 0;

    if (info_funcs == NULL) return ;

    for (i = INFO_FUNC_FOUND_ROWS; i < INFO_FUNC_MAX; i++) {
        if (info_funcs[i].field_name != NULL) {
            g_string_free(info_funcs[i].field_name, TRUE);
        }
    }

    g_free(info_funcs);
}

void
reset_funcs_info(info_func *info_funcs)
{

    gint i = 0;

    return ;

    if (info_funcs == NULL) return ;

    for (i = INFO_FUNC_FOUND_ROWS; i < INFO_FUNC_MAX; i++) {
        info_funcs[i].field_value = -1;
    }
}
