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
 

#ifndef _NETWORK_MYSQLD_H_
#define _NETWORK_MYSQLD_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TIME_H
/**
 * event.h needs struct timeval and doesn't include sys/time.h itself
 */
#include <sys/time.h>
#endif

#include <sys/types.h>

#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#include <winsock2.h>
#endif

#include <mysql.h>

#include <glib.h>

#include <execinfo.h>

#include "network-exports.h"

#include "network-socket.h"
#include "network-conn-pool.h"
#include "chassis-plugin.h"
#include "chassis-mainloop.h"
#include "chassis-timings.h"
#include "sys-pedantic.h"
#include "lua-scope.h"
#include "network-backend.h"
#include "lua-registry-keys.h"
#include "network-mysqld-stats.h"
#include "network-conn-errcode.h"

#define MAX_FRAMES 128

#define SEND_ERR_MSG_HANDLE(log_func, errmsg, con)                              \
do {                                                                            \
    log_func("%s(%s) event_thread(%d) "                                         \
        "C:%s S:%s(thread_id:%u) Usr:%s Db:%s %s",                              \
        G_STRLOC, __func__,                                                     \
        chassis_event_get_threadid(),                                           \
        NETWORK_SOCKET_SRC_NAME(con), NETWORK_SOCKET_DST_NAME(con),             \
        NETWORK_SOCKET_THREADID(con), NETWORK_SOCKET_USR_NAME(con),             \
        NETWORK_SOCKET_DB_NAME(con), errmsg ? errmsg : "NULL");                 \
} while (0)


typedef struct network_mysqld_con network_mysqld_con; /* forward declaration */

#undef NETWORK_MYSQLD_WANT_CON_TRACK_TIME
#ifdef NETWORK_MYSQLD_WANT_CON_TRACK_TIME
#define NETWORK_MYSQLD_CON_TRACK_TIME(con, name) chassis_timestamps_add(con->timestamps, name, __FILE__, __LINE__)
#else
#define NETWORK_MYSQLD_CON_TRACK_TIME(con, name) 
#endif

/**
 * A macro that produces a plugin callback function pointer declaration.
 */
#define NETWORK_MYSQLD_PLUGIN_FUNC(x) network_socket_retval_t (*x)(chassis *, network_mysqld_con *)
/**
 * The prototype for plugin callback functions.
 * 
 * Some plugins don't use the global "chas" pointer, thus it is marked "unused" for GCC.
 */
#define NETWORK_MYSQLD_PLUGIN_PROTO(x) static network_socket_retval_t x(chassis G_GNUC_UNUSED *chas, network_mysqld_con *con)

/**
 * The function pointers to plugin callbacks for each customizable state in the MySQL Protocol.
 * 
 * Any of these callbacks can be NULL, in which case the default pass-through behavior will be used.
 * 
 * The function prototype is defined by #NETWORK_MYSQLD_PLUGIN_PROTO, which is used in each plugin to define the callback.
 * #NETWORK_MYSQLD_PLUGIN_FUNC can be used to create a function pointer declaration.
 */
typedef struct {
    /**
     * Called when a new client connection to MySQL Proxy was created.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_init);
    /**
     * Called when MySQL Proxy needs to establish a connection to a backend server
     *
     * Returning a handshake response packet from this callback will cause the con_read_handshake step to be skipped.
     * The next state then is con_send_handshake.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_connect_server);
    /**
     * Called when MySQL Proxy has read the handshake packet from the server.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_handshake);
    /**
     * Called when MySQL Proxy wants to send the handshake packet to the client.
     * 
     * @note No known plugins actually implement this step right now, but rather return a handshake challenge from con_init instead.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_handshake);
    /**
     * Called when MySQL Proxy has read the authentication packet from the client.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_auth);
    /**
     * Called when MySQL Proxy wants to send the authentication packet to the server.
     * 
     * @note No known plugins actually implement this step.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_auth);
    /**
     * Called when MySQL Proxy has read the authentication result from the backend server, in response to con_send_auth.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_auth_result);
    /**
     * Called when MySQL Proxy wants to send the authentication response packet to the client.
     * 
     * @note No known plugins implement this callback, but the default implementation deals with the important case that
     * the authentication response used the pre-4.1 password hash method, but the client didn't.
     * @see network_mysqld_con::auth_result_state
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_auth_result);
    /**
     * Called when MySQL Proxy receives a COM_QUERY packet from a client.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_query);
    /**
     * Called when MySQL Proxy receives a result set from the server.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_query_result);
    /**
     * Called when MySQL Proxy sends a result set to the client.
     * 
     * The proxy plugin, for example, uses this state to inject more queries into the connection, possibly in response to a
     * result set received from a server.
     * 
     * This callback should not cause multiple result sets to be sent to the client.
     * @see network_mysqld_con_lua_injection::sent_resultset
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_query_result);
    /**
     * Called when an internal timer has elapsed.
     * 
     * This state is meant to give a plugin the opportunity to react to timers.
     * @note This state is currently unused, as there is no support for setting up timers.
     * @deprecated Unsupported, there is no way to set timers right now. Might be removed in 1.0.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_timer_elapsed);
    /**
     * Called when either side of a connection was either closed or some network error occurred.
     * 
     * Usually this is called because a client has disconnected. Plugins might want to preserve the server connection in this case
     * and reuse it later. In this case the connection state will be ::CON_STATE_CLOSE_CLIENT.
     * 
     * When an error on the server connection occurred, this callback is usually used to close the client connection as well.
     * In this case the connection state will be ::CON_STATE_CLOSE_SERVER.
     * 
     * @note There are no two separate callback functions for the two possibilities, which probably is a deficiency.
     */
    NETWORK_MYSQLD_PLUGIN_FUNC(con_cleanup);

    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_local_infile_data);
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_local_infile_data);
    NETWORK_MYSQLD_PLUGIN_FUNC(con_read_local_infile_result);
    NETWORK_MYSQLD_PLUGIN_FUNC(con_send_local_infile_result);

} network_mysqld_hooks;

/**
 * A structure containing the parsed packet for a command packet as well as the common parts necessary to find the correct
 * packet parsing function.
 * 
 * The correct parsing function is chose by looking at both the current state as well as the command in this structure.
 * 
 * @todo Currently the plugins are responsible for setting the first two fields of this structure. We have to investigate
 * how we can refactor this into a more generic way.
 */
struct network_mysqld_con_parse {
    enum enum_server_command command;   /**< The command indicator from the MySQL Protocol */

    gpointer data;                      /**< An opaque pointer to a parsed command structure */
    void (*data_free)(gpointer);        /**< A function pointer to the appropriate "free" function of data */
};

/**
 * The possible states in the MySQL Protocol.
 * 
 * Not all of the states map directly to plugin callbacks. Those states that have no corresponding plugin callbacks are marked as
 * <em>internal state</em>.
 */
typedef enum { 
    CON_STATE_INIT = 0,                  /**< A new client connection was established */
    CON_STATE_CONNECT_SERVER = 1,        /**< A connection to a backend is about to be made */
    CON_STATE_READ_HANDSHAKE = 2,        /**< A handshake packet is to be read from a server */
    CON_STATE_SEND_HANDSHAKE = 3,        /**< A handshake packet is to be sent to a client */
    CON_STATE_READ_AUTH = 4,             /**< An authentication packet is to be read from a client */
    CON_STATE_SEND_AUTH = 5,             /**< An authentication packet is to be sent to a server */
    CON_STATE_READ_AUTH_RESULT = 6,      /**< The result of an authentication attempt is to be read from a server */
    CON_STATE_SEND_AUTH_RESULT = 7,      /**< The result of an authentication attempt is to be sent to a client */
    CON_STATE_READ_AUTH_OLD_PASSWORD = 8,/**< The authentication method used is for pre-4.1 MySQL servers, internal state */
    CON_STATE_SEND_AUTH_OLD_PASSWORD = 9,/**< The authentication method used is for pre-4.1 MySQL servers, internal state */
    CON_STATE_READ_QUERY = 10,           /**< COM_QUERY packets are to be read from a client */
    CON_STATE_SEND_QUERY = 11,           /**< COM_QUERY packets are to be sent to a server */
    CON_STATE_READ_QUERY_RESULT = 12,    /**< Result set packets are to be read from a server */
    CON_STATE_SEND_QUERY_RESULT = 13,    /**< Result set packets are to be sent to a client */
    
    CON_STATE_CLOSE_CLIENT = 14,         /**< The client connection should be closed */
    CON_STATE_SEND_ERROR = 15,           /**< An unrecoverable error occurred, leads to sending a MySQL ERR packet to the client and closing the client connection */
    CON_STATE_ERROR = 16,                /**< An error occurred (malformed/unexpected packet, unrecoverable network error), internal state */

    CON_STATE_CLOSE_SERVER = 17,         /**< The server connection should be closed */

    /* handling the LOAD DATA LOCAL INFILE protocol extensions */
    CON_STATE_READ_LOCAL_INFILE_DATA = 18,
    CON_STATE_SEND_LOCAL_INFILE_DATA = 19,
    CON_STATE_READ_LOCAL_INFILE_RESULT = 20,
    CON_STATE_SEND_LOCAL_INFILE_RESULT = 21
} network_mysqld_con_state_t;

/**
 * get the name of a connection state
 */
NETWORK_API const char *network_mysqld_con_state_get_name(network_mysqld_con_state_t state);

typedef struct {
    guint sub_sql_num;
    guint sub_sql_exed;
    GPtrArray* rows;
    int limit;
    guint64 affected_rows;
    guint16 warnings;
} merge_res_t;

#define VALUE_IS_STRING 1
#define VALUE_IS_MINUS 2

typedef struct {
    GString *var_name;
    GString *var_value;
    guint var_value_extra;
} set_var_unit;

#define LOCK_TYPE_NONE 0
#define LOCK_TYPE_GET 1
#define LOCK_TYPE_RELEASE 2

#define CON_ALIVE_NORMAL    0
#define CON_EXIT_KILL       1
#define CON_EXIT_TX         2

/*
 * guint64 con_id =    THEARD_ID    | CON_IDX_THREAD
 *                  | .. 2 bytes .. | .. 6 bytes .. |
 */
#define CON_IDX_MASK    0xFFFFFFFFFFFF
#define CON_PRE_FIRST_ID    1

#define GET_THEAD_ID(X) ((X)>>48)
#define GET_CON_IDX(X) ((X) & CON_IDX_MASK)

#define MAKE_THEAD_ID(X) (((guint64)X)<<48)
#define MAKE_CON_ID(X, Y)  (MAKE_THEAD_ID(X) | GET_CON_IDX(Y))

#define INVALID_TYPE        0x00
#define DEFAULT_TYPE        0x01
#define WORK_TYPE           0x02
#define CHAIN_TYPE          0x04
#define NO_CHAIN_TYPE       0x08
#define RELEASE_TYPE        0x10
#define NO_RELEASE_TYPE     0x20

typedef enum INFO_FUNC_ID {
    INFO_FUNC_FOUND_ROWS = 0,
    INFO_FUNC_LAST_INSERT_ID,
    INFO_FUNC_ROW_COUNT,
    INFO_FUNC_MAX
} INFO_FUNC_ID;

typedef struct info_func {
    INFO_FUNC_ID info_func_id;
    GString        *field_name;
    gint          field_value;
} info_func;


typedef struct{
    gboolean is_in_transaction;                     // 当前是否在事务中
    gboolean is_set_autocommit;                     // 当前是否是set autocommit语句
    gboolean is_savepoint;
    gint is_commit;
    gint is_rollback;
    guint lock_stmt_type;                           // 当前是否是get_lock或者release_lock语句
    volatile gint  exit_phase;                       // 当前connnection的退出状态，使用在kill或者dbproxy shutdown过程
    volatile guint exit_begin_time;

    gboolean is_in_select_calc_found_rows;          // 当前是否是select_calc_found_rows
    gboolean is_found_rows;
    gboolean is_last_insert_id;
    gboolean is_row_count;
    info_func   *info_funcs;

    GString* use_db;                                // 当前use_db
    GString* lock_key;                              // 当前get_lock或者release_lock语句的lock key
    GString* set_charset_client;                    // 当前是否是set charset_client语句
    GString* set_charset_results;                   // 当前是否是set charset_results语句
    GString* set_charset_connection;                // 当前是否是set charset_connection语句
    GQueue* set_vars;                                // 当前是否是其他的set语句，可考虑与字符集合并
} connection_status_t;

typedef struct {
    guint64 ts_read_query;
    GString *cur_sql_rewrite;
    GString *cur_sql_rewrite_md5;
} conn_filter_t;

/**
 * Encapsulates the state and callback functions for a MySQL protocol-based connection to and from MySQL Proxy.
 * 
 * New connection structures are created by the function responsible for handling the accept on a listen socket, which
 * also is a network_mysqld_con structure, but only has a server set - there is no "client" for connections that we listen on.
 * 
 * The chassis itself does not listen on any sockets, this is left to each plugin. Plugins are free to create any number of
 * connections to listen on, but most of them will only create one and reuse the network_mysqld_con_accept function to set up an
 * incoming connection.
 * 
 * Each plugin can register callbacks for the various states in the MySQL Protocol, these are set in the member plugins.
 * A plugin is not required to implement any callbacks at all, but only those that it wants to customize. Callbacks that
 * are not set, will cause the MySQL Proxy core to simply forward the received data.
 */
struct network_mysqld_con {
    /**
     * The current/next state of this connection.
     */
    guint64    con_id;

    /* 
     * When the protocol state machine performs a transition, this variable will contain the next state,
     * otherwise, while performing the action at state, it will be set to the connection's current state
     * in the MySQL protocol.
     * 
     * Plugins may update it in a callback to cause an arbitrary state transition, however, this may result
     * reaching an invalid state leading to connection errors.
     * 
     * @see network_mysqld_con_handle
     */
    network_mysqld_con_state_t state;

    /* The current wait event of this connection. */
    network_mysqld_con_wait_t wait_status;

    /**
     * The server side of the connection as it pertains to the low-level network implementation.
     */
    network_socket *server;
    /**
     * The client side of the connection as it pertains to the low-level network implementation.
     */
    network_socket *client;

    /**
     * Function pointers to the plugin's callbacks.
     * 
     * Plugins don't need set any of these, but if unset, the plugin will not have the opportunity to
     * alter the behavior of the corresponding protocol state.
     * 
     * @note In theory you could use functions from different plugins to handle the various states, but there is no guarantee that
     * this will work. Generally the plugins will assume that config is their own chassis_plugin_config (a plugin-private struct)
     * and violating this constraint may lead to a crash.
     * @see chassis_plugin_config
     */
    network_mysqld_hooks plugins;
    
    /**
     * A pointer to a plugin-private struct describing configuration parameters.
     * 
     * @note The actual struct definition used is private to each plugin.
     */
    chassis_plugin_config *config;

    /**
     * A pointer back to the global, singleton chassis structure.
     */
    chassis *srv; /* our srv object */

    /**
     * A boolean flag indicating that this connection should only be used to accept incoming connections.
     * 
     * It does not follow the MySQL protocol by itself and its client network_socket will always be NULL.
     */
    int is_listen_socket;

    /**
     * An integer indicating the result received from a server after sending an authentication request.
     * 
     * This is used to differentiate between the old, pre-4.1 authentication and the new, 4.1+ one based on the response.
     */
    guint8 auth_result_state;

    /** Flag indicating if we the plugin doesn't need the resultset itself.
     * 
     * If set to TRUE, the plugin needs to see the entire resultset and we will buffer it.
     * If set to FALSE, the plugin is not interested in the content of the resultset and we'll
     * try to forward the packets to the client directly, even before the full resultset is parsed.
     */
    gboolean resultset_is_needed;
    /**
     * Flag indicating whether we have seen all parts belonging to one resultset.
     */
    gboolean resultset_is_finished;

    /**
     * Flag indicating that we have received a COM_QUIT command.
     * 
     * This is mainly used to differentiate between the case where the server closed the connection because of some error
     * or if the client asked it to close its side of the connection.
     * MySQL Proxy would report spurious errors for the latter case, if we failed to track this command.
     */
    gboolean com_quit_seen;

    /**
     * Contains the parsed packet.
     */
    struct network_mysqld_con_parse parse;

    /**
     * An opaque pointer to a structure describing extra connection state needed by the plugin.
     * 
     * The content and meaning is completely up to each plugin and the chassis will not access this in any way.
     * 
     * @note In practice, all current plugins and the chassis assume this to be network_mysqld_con_lua_t.
     */
    void *plugin_con_state;

    connection_status_var_t conn_status_var;

    connection_status_t conn_status;

    GHashTable* locks;

    merge_res_t* merge_res;

    GString* challenge;

    conn_filter_t con_filter_var;
    gboolean is_in_wait;
    gint    try_send_query_times;
    GRWLock server_lock;
    guint16 server_error_code;
};



NETWORK_API void g_list_string_free(gpointer data, gpointer UNUSED_PARAM(user_data));
NETWORK_API gboolean g_hash_table_true(gpointer UNUSED_PARAM(key), gpointer UNUSED_PARAM(value), gpointer UNUSED_PARAM(u));

NETWORK_API network_mysqld_con *network_mysqld_con_new(void);
NETWORK_API void network_mysqld_con_free(network_mysqld_con *con);

/** 
 * should be socket 
 */
NETWORK_API void network_mysqld_con_accept(int event_fd, short events, void *user_data); /** event handler for accept() */
NETWORK_API void network_mysqld_admin_con_accept(int event_fd, short events, void *user_data); /** event handler for accept() */

NETWORK_API int network_mysqld_con_send_ok(network_socket *con);
NETWORK_API int network_mysqld_con_send_ok_full(network_socket *con, guint64 affected_rows, guint64 insert_id, guint16 server_status, guint16 warnings);
NETWORK_API int network_mysqld_con_send_error(network_socket *con, const gchar *errmsg, gsize errmsg_len);
NETWORK_API int network_mysqld_con_send_error_full_nolog(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode, const gchar *sqlstate);
NETWORK_API int network_mysqld_con_send_error_pre41(network_socket *con, const gchar *errmsg, gsize errmsg_len);
NETWORK_API int network_mysqld_con_send_error_full_pre41(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode);
NETWORK_API int network_mysqld_con_send_resultset(network_socket *con, GPtrArray *fields, GPtrArray *rows);
NETWORK_API void network_mysqld_con_reset_command_response_state(network_mysqld_con *con);
NETWORK_API void network_mysqld_con_send_1_int_resultset(network_mysqld_con *con, gint info_type);
NETWORK_API gint64 network_mysqld_con_get_1_int_from_result_set(network_mysqld_con *con, void* inj_raw);
/**
 * should be socket 
 */
NETWORK_API network_socket_retval_t network_mysqld_read(chassis *srv, network_socket *con);
NETWORK_API network_socket_retval_t network_mysqld_write(chassis *srv, network_socket *con);
NETWORK_API network_socket_retval_t network_mysqld_write_len(chassis *srv, network_socket *con, int send_chunks);
NETWORK_API network_socket_retval_t network_mysqld_con_get_packet(chassis G_GNUC_UNUSED*chas, network_socket *con);

NETWORK_API int network_mysqld_init(chassis *srv, gchar *default_file, guint reomve_backend_timeout);
NETWORK_API void network_mysqld_add_connection(chassis *srv, network_mysqld_con *con);
NETWORK_API void network_mysqld_con_handle(int event_fd, short events, void *user_data);
NETWORK_API int network_mysqld_queue_append(network_socket *sock, network_queue *queue, const char *data, size_t len);
NETWORK_API int network_mysqld_queue_append_raw(network_socket *sock, network_queue *queue, GString *data);
NETWORK_API int network_mysqld_queue_reset(network_socket *sock);
NETWORK_API int network_mysqld_queue_sync(network_socket *dst, network_socket *src);

NETWORK_API void network_mysqld_send_query_stat(network_mysqld_con *con, char com_type, gboolean is_write);
NETWORK_API void network_mysqld_query_stat(network_mysqld_con *con, char com_type, gboolean is_write);

NETWORK_API void network_mysqld_stat_stmt_start(network_mysqld_con *con, const char *cur_query, gint com_type);
NETWORK_API void network_mysqld_stat_stmt_parser_end(network_mysqld_con *con, guint com_type, gboolean is_write);
NETWORK_API void network_mysqld_stat_stmt_end(network_mysqld_con *con, gint64 cur_time);

NETWORK_API void network_mysqld_socket_stat(chassis *chas, network_socket_dir_t socket_dir, gboolean is_write, guint len);

NETWORK_API set_var_unit *set_var_new();
NETWORK_API gint set_var_name_compare(set_var_unit *a, set_var_unit *b, void *user_data);
NETWORK_API gboolean set_var_value_eq(set_var_unit *a, set_var_unit *b);
NETWORK_API gint set_var_name_ge(set_var_unit *a, set_var_unit *b);
NETWORK_API set_var_unit * set_var_copy(set_var_unit *set_var, void *data);
NETWORK_API void set_var_print_set_value(GString *data, set_var_unit *set_var, gboolean set_default);
NETWORK_API void set_var_free(set_var_unit *set_var);

NETWORK_API void set_var_queue_merge(GQueue *set_vars, GQueue *to_merge_set_vars);
NETWORK_API void set_var_queue_insert(GQueue *set_vars, gchar *set_var_name, gchar *set_var_value, guint set_var_value_extra);

NETWORK_API void reset_funcs_info(info_func *info_funcs);

NETWORK_API gint kill_one_connection(chassis *chas, guint64 kill_con_id);

#define SEND_INTERNAL_ERR(msg)                                          \
do {                                                                    \
    gboolean valid = ISVALID(con->server_error_code);                                                           \
    gchar *errmsg = g_strdup_printf("Internal Error: %s", valid ? ERRMSG(con->server_error_code) : msg);        \
    network_mysqld_con_send_error_full_nolog(con->client,               \
                                            errmsg, strlen(errmsg),     \
                                            valid ? con->server_error_code : ER_UNKNOWN_ERROR,                  \
                                            valid ? SQLSTATE(con->server_error_code) : "07000");                \
    if (valid) con->server_error_code = 0;                                                                      \
    g_free(errmsg);                                                     \
} while (0)

#endif
