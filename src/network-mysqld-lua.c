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


#ifdef _WIN32
#include <winsock2.h> /* mysql.h needs SOCKET */
#endif

#include <mysql.h>
#include <mysqld_error.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "network-backend.h"
#include "glib-ext.h"
#include "lua-env.h"

#include "chassis-event-thread.h"
#include "chassis-filter.h"
#include "chassis-frontend.h"
#include "chassis-options-utils.h"

#include "network-mysqld.h"
#include "network-mysqld-packet.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-lua.h"
#include "network-socket-lua.h"
#include "network-backend-lua.h"
#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"
#include "network-injection-lua.h"
#include "chassis-log.h"
#include "lib/sql-tokenizer.h"

#define C(x) x, sizeof(x)-1
#define C_S(x) x, strlen(x)
#define S(x) x->str, x->len

/* reserved query */
typedef enum reserved_query_status
{
    IDX_RQ_NO_SATATUS = 0,
    IDX_RQ_FOBIDDEN_BY_FILTER,
    IDX_RQ_HIT_BY_FILTER,
    IDX_FILTER_AUTO_ADDED,
    IDX_FILTER_MANUAL_ADDED,
    IDX_RQ_ADD_FOR_SLOW_QUERY,
    IDX_RQ_ADD_FOR_FREQ_QUERY
} reserved_query_status;

const char *query_filter_status[] = {"normal",
                                    "forbidden by filter",
                                    "hit by filter",
                                    "auto added",
                                    "manual added",
                                    "add for slow query",
                                    "add for frequency query"};
static void show_reserved_query(GQueue *gl_reserved_query, lua_State *L, int begin_idx);
static gboolean try_get_int64_value(const gchar *option_value, gint64 *return_value);
static void send_options_to_lua(chassis_options_t *opts, const char *group_name,
                                        lua_State *L, chassis *chas, const char *var_name);
static gint show_variables(chassis *chas, lua_State *L, const char *option_value);

static chassis_option_t *find_option_by_name(const gchar *opt_name, chassis_options_t *opts);
static chassis_option_t *find_option_from_chas(chassis *chas, const gchar *opt_name);

static gint tables_opts_handle(chassis *chas, gint tables_opts_type, lua_State *L, const char *option_value);


network_mysqld_con_lua_t *network_mysqld_con_lua_new() {
    network_mysqld_con_lua_t *st;

    st = g_new0(network_mysqld_con_lua_t, 1);

    st->injected.queries = network_injection_queue_new();
    
    return st;
}

void network_mysqld_con_lua_free(network_mysqld_con_lua_t *st) {
    if (!st) return;

    if (st->backend != NULL)
    {
        g_atomic_int_dec_and_test(&st->backend->connected_clients);
    }

    network_injection_queue_free(st->injected.queries);

    g_free(st);
}


/**
 * get the connection information
 *
 * note: might be called in connect_server() before con->server is set 
 */
static int proxy_connection_get(lua_State *L) {
    network_mysqld_con *con = *(network_mysqld_con **)luaL_checkself(L); 
    network_mysqld_con_lua_t *st;
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    st = con->plugin_con_state;

    /**
     * we to split it in .client and .server here
     */

    if (strleq(key, keysize, C("default_db"))) {
        return luaL_error(L, "proxy.connection.default_db is deprecated, use proxy.connection.client.default_db or proxy.connection.server.default_db instead");
    } else if (strleq(key, keysize, C("thread_id"))) {
        return luaL_error(L, "proxy.connection.thread_id is deprecated, use proxy.connection.server.thread_id instead");
    } else if (strleq(key, keysize, C("mysqld_version"))) {
        return luaL_error(L, "proxy.connection.mysqld_version is deprecated, use proxy.connection.server.mysqld_version instead");
    } else if (strleq(key, keysize, C("backend_ndx"))) {
        lua_pushinteger(L, st->backend_ndx + 1);
    } else if ((con->server && (strleq(key, keysize, C("server")))) ||
               (con->client && (strleq(key, keysize, C("client"))))) {
        network_socket **socket_p;

        socket_p = lua_newuserdata(L, sizeof(network_socket)); /* the table underneat proxy.socket */

        if (key[0] == 's') {
            *socket_p = con->server;
        } else {
            *socket_p = con->client;
        }

        network_socket_lua_getmetatable(L);
        lua_setmetatable(L, -2); /* tie the metatable to the table   (sp -= 1) */
    } else if ((con->server && (strleq(key, keysize, C("server")))) ||
               (con->client && (strleq(key, keysize, C("client"))))) {
        network_socket **socket_p;

        socket_p = lua_newuserdata(L, sizeof(network_socket)); /* the table underneat proxy.socket */

        if (key[0] == 's') {
            *socket_p = con->server;
        } else {
            *socket_p = con->client;
        }

        network_socket_lua_getmetatable(L);
        lua_setmetatable(L, -2); /* tie the metatable to the table   (sp -= 1) */
    } else {
        lua_pushnil(L);
    }

    return 1;
}

/**
 * set the connection information
 *
 * note: might be called in connect_server() before con->server is set 
 */
static int proxy_connection_set(lua_State *L) {
    network_mysqld_con *con = *(network_mysqld_con **)luaL_checkself(L);
    network_mysqld_con_lua_t *st;
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    st = con->plugin_con_state;

    if (strleq(key, keysize, C("backend_ndx"))) {
        /**
         * in lua-land the ndx is based on 1, in C-land on 0 */
        int backend_ndx = luaL_checkinteger(L, 3) - 1;
        network_socket *send_sock;
            
        if (backend_ndx == -1) {
            /** drop the backend for now
             */
            network_connection_pool_lua_add_connection(con);
        } else if (NULL != (send_sock = network_connection_pool_lua_swap(con, network_backends_get(con->srv->backends, backend_ndx), backend_ndx, NULL))) {
            con->server = send_sock;
        } else if (backend_ndx == -2) {
            if (st->backend != NULL) {
                g_atomic_int_dec_and_test(&st->backend->connected_clients);
                st->backend = NULL;
            }
            st->backend_ndx = -1;
            g_rw_lock_writer_lock(&con->server_lock);
            network_socket_free(con->server);
            con->server = NULL;
            g_rw_lock_writer_unlock(&con->server_lock);

        } else {
            st->backend_ndx = backend_ndx;
        }
    } else if (0 == strcmp(key, "connection_close")) {
        luaL_checktype(L, 3, LUA_TBOOLEAN);

        st->connection_close = lua_toboolean(L, 3);
    } else {
        return luaL_error(L, "proxy.connection.%s is not writable", key);
    }

    return 0;
}

int network_mysqld_con_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_connection_get },
        { "__newindex", proxy_connection_set },
        { NULL, NULL },
    };
    return proxy_getmetatable(L, methods);
}


static int network_mysqld_status_get(lua_State *L) {
    chassis *chas = *(chassis **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    gint event_index = 0, stat_index =0, backend_index = 0;
    gdouble percentile = 0.0;

    if (strleq(key, keysize, C("proxy_status"))) {
        guint64 status_sum = 0;
        gint socket_num = 0, cached_socket_num = 0;

        lua_newtable(L);
        for (stat_index = 0; stat_index < THREAD_STAT_END; stat_index++)
        {
            for (event_index = 0; event_index <= chas->event_thread_count; event_index++)
            {
                chassis_event_thread_t *event_thread = g_ptr_array_index(chas->threads, event_index);
                status_sum += event_thread->thread_status_var.thread_stat[stat_index];
            }

            lua_pushstring(L, network_mysqld_stat_desc[stat_index]);
            lua_pushnumber(L, status_sum);
            lua_settable(L,-3);

            status_sum = 0;
        }

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_MAX_CONNECTIONS]);
        lua_pushnumber(L, chas->proxy_max_connections);
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_MAX_USED_CONNECTIONS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_max_used_connections));
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_CUR_CONNECTIONS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_connections));
        lua_settable(L,-3);

        g_rw_lock_reader_lock(&chas->backends->backends_lock);  /*remove lock*/
        for (backend_index = 0; backend_index < chas->backends->backends->len; backend_index++)
        {
            network_backend_t *backend = g_ptr_array_index(chas->backends->backends, backend_index);
            socket_num += backend->connected_clients;

            for (event_index = 0; event_index <= chas->event_thread_count; event_index++)
            {
                network_connection_pool* pool = g_ptr_array_index(backend->pools, event_index);
                cached_socket_num += g_hash_table_size(pool);
            }
        }
        g_rw_lock_reader_unlock(&chas->backends->backends_lock);    /*remove lock*/

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_DB_CONNECTIONS]);
        lua_pushnumber(L, socket_num);
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_DB_CONNECTIONS_CACHED]);
        lua_pushnumber(L, cached_socket_num);
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_ATTEMPED_CONNECTS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_attempted_connects));
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_ABORTED_CONNECTS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_aborted_connects));
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_CLOSED_CLIENTS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_closed_clients));
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[GLOBAL_STAT_ABORTED_CLIENTS]);
        lua_pushnumber(L, g_atomic_int_get(&chas->proxy_aborted_clients));
        lua_settable(L,-3);

        lua_pushstring(L, network_mysqld_stat_desc[PERCENTILE]);
        {
            chassis_option_t    *opt = find_option_from_chas(chas, "percentile");
            external_param      *st_param = g_new0(external_param, 1);
            gchar               *percentil = NULL;

            st_param->magic_value = "1m";
            percentil = opt->show_hook((void *)st_param);
            lua_pushstring(L, percentil);
            g_free(percentil);
            g_free(st_param);
        }
        lua_settable(L, -3);
    } else if (strleq(key, keysize, C("wait_event_stat"))) {
        gint event_index = 0, wait_event_index =0;
        wait_event_stat_t wait_event_stat;

        lua_newtable(L);

        for (wait_event_index = 0; wait_event_index < WAIT_EVENT_END; wait_event_index++)
        {
            memset(&wait_event_stat, 0, sizeof(wait_event_stat_t));

            for (event_index = 0; event_index <= chas->event_thread_count; event_index++)
            {
                chassis_event_thread_t *event_thread = g_ptr_array_index(chas->threads, event_index);
                if (event_thread->thread_status_var.wait_event_stat[wait_event_index].waits > 0)
                {
                    wait_event_stat.waits += event_thread->thread_status_var.wait_event_stat[wait_event_index].waits;
                    wait_event_stat.longer_waits += event_thread->thread_status_var.wait_event_stat[wait_event_index].longer_waits;

                    if (wait_event_stat.min_wait_time ==0 || event_thread->thread_status_var.wait_event_stat[wait_event_index].min_wait_time < wait_event_stat.min_wait_time)
                    {
                        wait_event_stat.min_wait_time = event_thread->thread_status_var.wait_event_stat[wait_event_index].min_wait_time;
                    }

                    if (event_thread->thread_status_var.wait_event_stat[wait_event_index].max_wait_time > wait_event_stat.max_wait_time)
                    {
                        wait_event_stat.max_wait_time = event_thread->thread_status_var.wait_event_stat[wait_event_index].max_wait_time;
                    }

                    wait_event_stat.total_wait_time += event_thread->thread_status_var.wait_event_stat[wait_event_index].total_wait_time;
                    wait_event_stat.total_longer_wait_time += event_thread->thread_status_var.wait_event_stat[wait_event_index].total_longer_wait_time;
                }
            }

            lua_pushnumber(L, wait_event_index);

            lua_newtable(L);

            lua_pushnumber(L, 0);
            lua_pushstring(L, chas_thread_wait_event_desc[wait_event_index]);
            lua_settable(L,-3);

            lua_pushnumber(L, 1);
            lua_pushnumber(L, wait_event_stat.waits);
            lua_settable(L,-3);

            lua_pushnumber(L, 2);
            lua_pushnumber(L, wait_event_stat.longer_waits);
            lua_settable(L,-3);

            lua_pushnumber(L, 3);
            lua_pushnumber(L, wait_event_stat.min_wait_time);
            lua_settable(L,-3);

            lua_pushnumber(L, 4);
            lua_pushnumber(L, wait_event_stat.max_wait_time);
            lua_settable(L,-3);

            lua_pushnumber(L, 5);
            lua_pushnumber(L, wait_event_stat.total_wait_time);
            lua_settable(L,-3);

            lua_pushnumber(L, 6);
            lua_pushnumber(L, wait_event_stat.total_longer_wait_time);
            lua_settable(L,-3);

            lua_settable(L, -3);

        }
    }  else if (strleq(key, keysize, C("processlist"))) {
        network_mysqld_con_state_t state;
        network_mysqld_con_wait_t wait_status;
        gint event_index = 0, conn_index = 0;
        network_mysqld_con *conn = NULL;
        GList *gl_conn = NULL;
        guint64 query_time = 0;
        guint64 cur_time_in_micro_s = chassis_get_rel_microseconds();

        lua_newtable(L);

        for (event_index = 0; event_index <= chas->event_thread_count; event_index++)
        {
            chassis_event_thread_t *event_thread = g_ptr_array_index(chas->threads, event_index);
            g_rw_lock_reader_lock(&event_thread->connection_lock);
            if (event_thread->connection_list != NULL)
            {
                gl_conn = event_thread->connection_list;

                while (gl_conn)
                {
                    gchar con_id_str[32] = "";
                    conn = gl_conn->data;
                    sprintf(con_id_str, "%llu", conn->con_id);
                    lua_pushlstring(L, C_S(con_id_str));

                    lua_newtable(L);

                    lua_pushstring(L, "user");
                    lua_pushlstring(L, C_S(NETWORK_SOCKET_USR_NAME(conn->client)));
                    lua_settable(L,-3);

                    lua_pushstring(L, "event_thread");
                    lua_pushnumber(L, event_index);
                    lua_settable(L,-3);

                    lua_pushstring(L, "host");
                    lua_pushlstring(L, C_S(NETWORK_SOCKET_SRC_NAME(conn->client)));
                    lua_settable(L,-3);

                    lua_pushstring(L, "db");
                    if (conn->client->conn_attr.default_db->len > 0) {
                        lua_pushlstring(L, S(conn->client->conn_attr.default_db));
                    } else {
                        lua_pushlstring(L, C("NULL"));
                    }
                    lua_settable(L,-3);

                    g_rw_lock_reader_lock(&conn->server_lock);
                    lua_pushstring(L, "backend");
                    if (conn->server && conn->server->dst->name) {
                        lua_pushlstring(L, S(conn->server->dst->name));
                    } else {
                        lua_pushlstring(L, C("NULL"));
                    }
                    lua_settable(L,-3);

                    lua_pushstring(L, "thread_id");
                    if (conn->server && conn->server->challenge) {
                        lua_pushnumber(L, NETWORK_SOCKET_THREADID(conn->server));
                    } else {
                        lua_pushnumber(L, 0);
                    }
                    lua_settable(L,-3);
                    g_rw_lock_reader_unlock(&conn->server_lock);

                    query_time = 0;
                    lua_pushstring(L, "time");
                    if (event_index > 0 && conn->conn_status_var.cur_query_start_time != 0) {
                        if (cur_time_in_micro_s > conn->conn_status_var.cur_query_start_time) {
                            query_time = (guint64)(cur_time_in_micro_s - conn->conn_status_var.cur_query_start_time)/MICROSEC;
                        }
                    }
                    lua_pushnumber(L, query_time);
                    lua_settable(L,-3);

                    lua_pushstring(L, "info");
                    if (event_index > 0 && strlen(conn->conn_status_var.cur_query) > 0) {
                        lua_pushlstring(L, C_S(conn->conn_status_var.cur_query));
                    } else {
                        lua_pushlstring(L, C("SLEEP"));
                    }
                    lua_settable(L,-3);

                    state = conn->state;
                    lua_pushstring(L, "state");
                    lua_pushstring(L, state <= CON_STATE_SEND_LOCAL_INFILE_RESULT ? network_mysqld_conn_stat_desc[conn->state] : "Invalid state");
                    lua_settable(L, -3);

                    wait_status = conn->wait_status;
                    lua_pushstring(L, "wait_status");
                    lua_pushstring(L, wait_status < CON_WAIT_END ? network_mysqld_conn_wait_desc[conn->wait_status] : "Invalid state");
                    lua_settable(L, -3);


                    lua_pushstring(L, "auto_commit");
                    lua_pushstring(L, conn->client->conn_attr.autocommit_status == AUTOCOMMIT_FALSE ? "false" : "true");
                    lua_settable(L, -3);

                    lua_pushstring(L, "in_trx");
                    lua_pushstring(L, conn->conn_status.is_in_transaction ? "true" : "false");
                    lua_settable(L, -3);

                    lua_pushstring(L, "db_connected");
                    lua_pushstring(L, conn->server != NULL ? "true" : "false");
                    lua_settable(L, -3);

                    lua_settable(L, -3);

                    gl_conn = g_list_next(gl_conn);
                    conn_index++;
                }
            }
            g_rw_lock_reader_unlock(&event_thread->connection_lock);
        }
    } else if (strleq(key, keysize, C("query_response_time"))) {
        response_time_stat_t response_time_stat, *p_cur_response_time_stat;
        gint stat_index = 0, query_type = 0;

        lua_newtable(L);

        for (stat_index = 0; stat_index < MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS + 1; stat_index++)
        {
            if (chas->query_response_time_stats != 2 && stat_index > 0) continue;

            for (query_type = 0; query_type < QUERY_TYPE_NUM; query_type++)
            {
                memset(&response_time_stat, 0, sizeof(response_time_stat_t));
                for (event_index = 0; event_index <= chas->event_thread_count; event_index++)
                {
                    chassis_event_thread_t *event_thread = g_ptr_array_index(chas->threads, event_index);
                    p_cur_response_time_stat = stat_index > 0 ?
                            &(event_thread->thread_status_var.hist_query_rt_stat[query_type][stat_index - 1]) : &(event_thread->thread_status_var.all_query_rt_stat[query_type]);

                    response_time_stat.query_num += p_cur_response_time_stat->query_num;
                    response_time_stat.total_respon_time += p_cur_response_time_stat->total_respon_time;

                    if (p_cur_response_time_stat->query_num > 0)
                    {
                        SET_MIN(response_time_stat.min_respon_time, p_cur_response_time_stat->min_respon_time);
                        SET_MAX(response_time_stat.max_respon_time, p_cur_response_time_stat->max_respon_time);
                    }
                }

                lua_pushnumber(L, (stat_index * QUERY_TYPE_NUM) + query_type);

                lua_newtable(L);

                lua_pushnumber(L, 0);
                if (stat_index == 0) {
                    lua_pushstring(L, "ALL");
                } else if (stat_index == MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS){
                    lua_pushstring(L, "TOO LONG");
                } else {
                    lua_pushnumber(L, pow(chas->query_response_time_range_base, stat_index - 1));
                }
                lua_settable(L,-3);

                lua_pushnumber(L, 1);
                lua_pushstring(L, network_mysqld_query_type_desc[query_type]);
                lua_settable(L,-3);

                lua_pushnumber(L, 2);
                lua_pushnumber(L, response_time_stat.query_num);
                lua_settable(L,-3);

                lua_pushnumber(L, 3);
                lua_pushnumber(L, response_time_stat.total_respon_time);
                lua_settable(L,-3);

                lua_pushnumber(L, 4);
                lua_pushnumber(L, response_time_stat.min_respon_time);
                lua_settable(L,-3);

                lua_pushnumber(L, 5);
                lua_pushnumber(L, response_time_stat.max_respon_time);
                lua_settable(L,-3);

                lua_settable(L, -3);
            }
        }
    }
    else if (strleq(key, keysize, C("lastest_queries")))
    {
        int query_num = 0;
        lua_newtable(L);

        g_rw_lock_reader_lock(&chas->proxy_reserved->rq_lock);
        show_reserved_query(chas->proxy_reserved->gq_reserved_long_query, L, 0);
        query_num = g_queue_get_length(chas->proxy_reserved->gq_reserved_long_query);
        show_reserved_query(chas->proxy_reserved->gq_reserved_short_query, L, query_num);
        g_rw_lock_reader_unlock(&chas->proxy_reserved->rq_lock);
    }
    else if (strleq(key, keysize, C("blacklists")))
    {
        int filter_sum = 0;
        lua_newtable(L);

        if (chas->proxy_filter != NULL)
        {
            g_rw_lock_reader_lock(&chas->proxy_filter->sql_filter_lock);
            sql_filter *gl_sql_filter = chas->proxy_filter;
            GHashTableIter iter;
            gchar   *key;
            sql_filter_hval *value;

            g_hash_table_iter_init (&iter, gl_sql_filter->blacklist);
            while (g_hash_table_iter_next (&iter, (gpointer)&key, (gpointer)&value))
            {
                lua_pushnumber(L, filter_sum);
                lua_newtable(L);

                lua_pushstring(L, "filter_hashcode");
                lua_pushlstring(L, C_S(key));
                lua_settable(L,-3);

                lua_pushstring(L, "filter");
                lua_pushlstring(L, C_S(value->sql_filter_item));
                lua_settable(L,-3);

                lua_pushstring(L, "is_enabled");
                lua_pushnumber(L, value->flag);
                lua_settable(L,-3);

                lua_pushstring(L, "filter_status");
                if (value->filter_status & AUTO_ADD_FILTER)
                    lua_pushlstring(L, C_S(query_filter_status[IDX_FILTER_AUTO_ADDED]));
                else
                    lua_pushlstring(L, C_S(query_filter_status[IDX_FILTER_MANUAL_ADDED]));
                lua_settable(L,-3);

                lua_pushstring(L, "hit_times");
                lua_pushnumber(L, value->hit_times);
                lua_settable(L,-3);

                lua_settable(L,-3);
                filter_sum++;
            }
            g_rw_lock_reader_unlock(&chas->proxy_filter->sql_filter_lock);
        }
    }

    return 1;
}

static int network_mysqld_status_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", network_mysqld_status_get },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}

/**
 * Set up the global structures for a script.
 * 
 * @see lua_register_callback - for connection local setup
 */
void network_mysqld_lua_setup_global(lua_State *L , chassis *chas) {
    network_backends_t **backends_p;

    int stack_top = lua_gettop(L);

    /* TODO: if we share "proxy." with other plugins, this may fail to initialize it correctly, 
     * because maybe they already have registered stuff in there.
     * It would be better to have different namespaces, or any other way to make sure we initialize correctly.
     */
    lua_getglobal(L, "proxy");
    if (lua_isnil(L, -1)) {
        lua_pop(L, 1);

        network_mysqld_lua_init_global_fenv(L);
    
        lua_getglobal(L, "proxy");
    }
    g_assert(lua_istable(L, -1));

    /* at this point we have set up:
     *  - the script
     *  - _G.proxy and a bunch of constants in that table
     *  - _G.proxy.global
     */
    
    /**
     * register proxy.global.backends[]
     *
     * @see proxy_backends_get()
     */
    lua_getfield(L, -1, "global");

    // set instance name
    // proxy.global.config.instance , value assigned when cmd start use --instance
    lua_getfield(L, -1, "config");

    lua_pushstring(L, chas->instance_name);
    lua_setfield(L, -2, "instance");

    lua_pushstring(L, chas->log_path);
    lua_setfield(L, -2, "logpath");

    lua_pop(L, 1);

    // 
    backends_p = lua_newuserdata(L, sizeof(network_backends_t *));
    *backends_p = chas->backends;

    network_backends_lua_getmetatable(L);
    lua_setmetatable(L, -2);          /* tie the metatable to the table   (sp -= 1) */

    lua_setfield(L, -2, "backends");

    GPtrArray **raw_ips_p = lua_newuserdata(L, sizeof(GPtrArray *));
    *raw_ips_p = chas->backends->raw_ips;
    network_clients_lua_getmetatable(L);
    lua_setmetatable(L, -2);
    lua_setfield(L, -2, "clients");

    network_backends_t **raw_pwds_p = lua_newuserdata(L, sizeof(network_backends_t *));
    *raw_pwds_p = chas->backends;
    network_pwds_lua_getmetatable(L);
    lua_setmetatable(L, -2);
    lua_setfield(L, -2, "pwds");


    chassis **chas_p = lua_newuserdata(L, sizeof(chassis *));
    *chas_p = chas;

    network_mysqld_status_lua_getmetatable(L);
    lua_setmetatable(L, -2);          /* tie the metatable to the table   (sp -= 1) */
    lua_setfield(L, -2, "status");

    chassis **chas_p3 = lua_newuserdata(L, sizeof(chassis *));
    *chas_p3 = chas;
    network_sys_config_lua_getmetatable(L);
    lua_setmetatable(L, -2);
    lua_setfield(L, -2, "sys_config");


    lua_pop(L, 2);  /* _G.proxy.global and _G.proxy */

    g_assert(lua_gettop(L) == stack_top);
}


/**
 * Load a lua script and leave the wrapper function on the stack.
 *
 * @return 0 on success, -1 on error
 */
int network_mysqld_lua_load_script(lua_scope *sc, const char *lua_script) {
    int stack_top = lua_gettop(sc->L);

    if (!lua_script) return -1;
    
    /* a script cache
     *
     * we cache the scripts globally in the registry and move a copy of it 
     * to the new script scope on success.
     */
    lua_scope_load_script(sc, lua_script);

    if (lua_isstring(sc->L, -1)) {
        g_log_dbproxy(g_critical, "lua_load_file(%s) failed: %s", 
                lua_script, lua_tostring(sc->L, -1));

        lua_pop(sc->L, 1); /* remove the error-msg from the stack */
        
        return -1;
    } else if (!lua_isfunction(sc->L, -1)) {
        g_log_dbproxy(g_error, "luaL_loadfile(%s): returned a %s", 
                lua_script, lua_typename(sc->L, lua_type(sc->L, -1)));
    }

    g_assert(lua_gettop(sc->L) - stack_top == 1);

    return 0;
}

/**
 * setup the local script environment before we call the hook function
 *
 * has to be called before any lua_pcall() is called to start a hook function
 *
 * - we use a global lua_State which is split into child-states with lua_newthread()
 * - luaL_ref() moves the state into the registry and cleans up the global stack
 * - on connection close we call luaL_unref() to hand the thread to the GC
 *
 * @see proxy_lua_free_script
 *
 *
 * if the script is cached we have to point the global proxy object
 *
 * @retval 0 success (even if we do not have a script)
 * @retval -1 The script failed to load, most likely because of a syntax error.
 * @retval -2 The script failed to execute.
 */
network_mysqld_register_callback_ret network_mysqld_con_lua_register_callback(network_mysqld_con *con, const char *lua_script) {
    lua_State *L = NULL;
    network_mysqld_con_lua_t *st   = con->plugin_con_state;

    lua_scope  *sc = con->srv->sc;

    GQueue **q_p;
    network_mysqld_con **con_p;
    int stack_top;

    if (!lua_script) return REGISTER_CALLBACK_SUCCESS;

    if (st->L) {
        /* we have to rewrite _G.proxy to point to the local proxy */
        L = st->L;

        g_assert(lua_isfunction(L, -1));

        lua_getfenv(L, -1);
        g_assert(lua_istable(L, -1));

        lua_getglobal(L, "proxy");
        lua_getmetatable(L, -1); /* meta(_G.proxy) */

        lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
        lua_setfield(L, -2, "__index"); /* meta[_G.proxy].__index = fenv.__proxy */

        lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
        lua_setfield(L, -2, "__newindex"); /* meta[_G.proxy].__newindex = fenv.__proxy */

        lua_pop(L, 3);

        g_assert(lua_isfunction(L, -1));

        return REGISTER_CALLBACK_SUCCESS; /* the script-env already setup, get out of here */
    }

    /* handles loading the file from disk/cache*/
    if (0 != network_mysqld_lua_load_script(sc, lua_script)) {
        /* loading script failed */
        return REGISTER_CALLBACK_LOAD_FAILED;
    }

    /* sets up global tables */
    network_mysqld_lua_setup_global(sc->L, con->srv);

    /**
     * create a side thread for this connection
     *
     * (this is not pre-emptive, it is just a new stack in the global env)
     */
    L = lua_newthread(sc->L);

    st->L_ref = luaL_ref(sc->L, LUA_REGISTRYINDEX);

    stack_top = lua_gettop(L);

    /* get the script from the global stack */
    lua_xmove(sc->L, L, 1);
    g_assert(lua_isfunction(L, -1));

    lua_newtable(L); /* my empty environment aka {}              (sp += 1) 1 */

    lua_newtable(L); /* the meta-table for the new env           (sp += 1) 2 */

    lua_pushvalue(L, LUA_GLOBALSINDEX);                       /* (sp += 1) 3 */
    lua_setfield(L, -2, "__index"); /* { __index = _G }          (sp -= 1) 2 */
    lua_setmetatable(L, -2); /* setmetatable({}, {__index = _G}) (sp -= 1) 1 */

    lua_newtable(L); /* __proxy = { }                            (sp += 1) 2 */

    g_assert(lua_istable(L, -1));

    q_p = lua_newuserdata(L, sizeof(GQueue *));               /* (sp += 1) 3 */
    *q_p = st->injected.queries;

    /*
     * proxy.queries
     *
     * implement a queue
     *
     * - append(type, query)
     * - prepend(type, query)
     * - reset()
     * - len() and #proxy.queue
     *
     */
    proxy_getqueuemetatable(L);

    lua_pushvalue(L, -1); /* meta.__index = meta */
    lua_setfield(L, -2, "__index");

    lua_setmetatable(L, -2);


    lua_setfield(L, -2, "queries"); /* proxy.queries = <userdata> */

    /*
     * proxy.connection is (mostly) read-only
     *
     * .thread_id  = ... thread-id against this server
     * .backend_id = ... index into proxy.global.backends[ndx]
     *
     */

    con_p = lua_newuserdata(L, sizeof(con));                          /* (sp += 1) */
    *con_p = con;

    network_mysqld_con_getmetatable(L);
    lua_setmetatable(L, -2);          /* tie the metatable to the udata   (sp -= 1) */


    

    lua_setfield(L, -2, "connection"); /* proxy.connection = <udata>     (sp -= 1) */

    /*
     * proxy.response knows 3 fields with strict types:
     *
     * .type = <int>
     * .errmsg = <string>
     * .resultset = { 
     *   fields = { 
     *     { type = <int>, name = <string > }, 
     *     { ... } }, 
     *   rows = { 
     *     { ..., ... }, 
     *     { ..., ... } }
     * }
     */
    lua_newtable(L);
#if 0
    lua_newtable(L); /* the meta-table for the response-table    (sp += 1) */
    lua_pushcfunction(L, response_get);                       /* (sp += 1) */
    lua_setfield(L, -2, "__index");                           /* (sp -= 1) */
    lua_pushcfunction(L, response_set);                       /* (sp += 1) */
    lua_setfield(L, -2, "__newindex");                        /* (sp -= 1) */
    lua_setmetatable(L, -2); /* tie the metatable to response    (sp -= 1) */
#endif
    lua_setfield(L, -2, "response");

    lua_setfield(L, -2, "__proxy");

    /* patch the _G.proxy to point here */
    lua_getglobal(L, "proxy");
    g_assert(lua_istable(L, -1));

    if (0 == lua_getmetatable(L, -1)) { /* meta(_G.proxy) */
        /* no metatable yet */

        lua_newtable(L);
    }
    g_assert(lua_istable(L, -1));

    lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
    g_assert(lua_istable(L, -1));
    lua_setfield(L, -2, "__index"); /* meta[_G.proxy].__index = fenv.__proxy */

    lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
    lua_setfield(L, -2, "__newindex"); /* meta[_G.proxy].__newindex = fenv.__proxy */

    lua_setmetatable(L, -2);

    lua_pop(L, 1);  /* _G.proxy */

    g_assert(lua_isfunction(L, -2));
    g_assert(lua_istable(L, -1));

    lua_setfenv(L, -2); /* on the stack should be a modified env (sp -= 1) */

    /* cache the script in this connection */
    g_assert(lua_isfunction(L, -1));
    lua_pushvalue(L, -1);

    /* run the script once to get the functions set in the global scope */
    if (lua_pcall(L, 0, 0, 0) != 0) {
        g_log_dbproxy(g_critical, "(lua-error) [%s]\n%s", lua_script, lua_tostring(L, -1));

        lua_pop(L, 1); /* errmsg */

        luaL_unref(sc->L, LUA_REGISTRYINDEX, st->L_ref);

        return REGISTER_CALLBACK_EXECUTE_FAILED;
    }

    st->L = L;

    g_assert(lua_isfunction(L, -1));
    g_assert(lua_gettop(L) - stack_top == 1);

    return REGISTER_CALLBACK_SUCCESS;
}

/**
 * init the global proxy object 
 */
void network_mysqld_lua_init_global_fenv(lua_State *L) {
    
    lua_newtable(L); /* my empty environment aka {}              (sp += 1) */
#define DEF(x) \
    lua_pushinteger(L, x); \
    lua_setfield(L, -2, #x);
    
    DEF(PROXY_SEND_QUERY);
    DEF(PROXY_SEND_RESULT);
    DEF(PROXY_IGNORE_RESULT);

    DEF(MYSQLD_PACKET_OK);
    DEF(MYSQLD_PACKET_ERR);
    DEF(MYSQLD_PACKET_RAW);

    DEF(BACKEND_STATE_UNKNOWN);
    DEF(BACKEND_STATE_UP);
    DEF(BACKEND_STATE_PENDING);
    DEF(BACKEND_STATE_DOWN);
    DEF(BACKEND_STATE_OFFLINING);
    DEF(BACKEND_STATE_OFFLINE);
    DEF(BACKEND_STATE_REMOVING);

    DEF(ADD_PWD);
    DEF(ADD_ENPWD);
    DEF(REMOVE_PWD);
    DEF(ADD_USER_HOST);
    DEF(REMOVE_USER_HOST);
    DEF(ADD_BACKENDS);
    DEF(REMOVE_BACKENDS);
    DEF(BACKEND_STATE);
    DEF(ADD_ADMIN_HOSTS);
    DEF(REMOVE_ADMIN_HOSTS);
    DEF(ALTTER_ADMIN_PWDS);

    DEF(BACKEND_TYPE_UNKNOWN);
    DEF(BACKEND_TYPE_RW);
    DEF(BACKEND_TYPE_RO);

    DEF(COM_SLEEP);
    DEF(COM_QUIT);
    DEF(COM_INIT_DB);
    DEF(COM_QUERY);
    DEF(COM_FIELD_LIST);
    DEF(COM_CREATE_DB);
    DEF(COM_DROP_DB);
    DEF(COM_REFRESH);
    DEF(COM_SHUTDOWN);
    DEF(COM_STATISTICS);
    DEF(COM_PROCESS_INFO);
    DEF(COM_CONNECT);
    DEF(COM_PROCESS_KILL);
    DEF(COM_DEBUG);
    DEF(COM_PING);
    DEF(COM_TIME);
    DEF(COM_DELAYED_INSERT);
    DEF(COM_CHANGE_USER);
    DEF(COM_BINLOG_DUMP);
    DEF(COM_TABLE_DUMP);
    DEF(COM_CONNECT_OUT);
    DEF(COM_REGISTER_SLAVE);
    DEF(COM_STMT_PREPARE);
    DEF(COM_STMT_EXECUTE);
    DEF(COM_STMT_SEND_LONG_DATA);
    DEF(COM_STMT_CLOSE);
    DEF(COM_STMT_RESET);
    DEF(COM_SET_OPTION);
#if MYSQL_VERSION_ID >= 50000
    DEF(COM_STMT_FETCH);
#if MYSQL_VERSION_ID >= 50100
    DEF(COM_DAEMON);
#endif
#endif
    DEF(MYSQL_TYPE_DECIMAL);
#if MYSQL_VERSION_ID >= 50000
    DEF(MYSQL_TYPE_NEWDECIMAL);
#endif
    DEF(MYSQL_TYPE_TINY);
    DEF(MYSQL_TYPE_SHORT);
    DEF(MYSQL_TYPE_LONG);
    DEF(MYSQL_TYPE_FLOAT);
    DEF(MYSQL_TYPE_DOUBLE);
    DEF(MYSQL_TYPE_NULL);
    DEF(MYSQL_TYPE_TIMESTAMP);
    DEF(MYSQL_TYPE_LONGLONG);
    DEF(MYSQL_TYPE_INT24);
    DEF(MYSQL_TYPE_DATE);
    DEF(MYSQL_TYPE_TIME);
    DEF(MYSQL_TYPE_DATETIME);
    DEF(MYSQL_TYPE_YEAR);
    DEF(MYSQL_TYPE_NEWDATE);
    DEF(MYSQL_TYPE_ENUM);
    DEF(MYSQL_TYPE_SET);
    DEF(MYSQL_TYPE_TINY_BLOB);
    DEF(MYSQL_TYPE_MEDIUM_BLOB);
    DEF(MYSQL_TYPE_LONG_BLOB);
    DEF(MYSQL_TYPE_BLOB);
    DEF(MYSQL_TYPE_VAR_STRING);
    DEF(MYSQL_TYPE_STRING);
    DEF(MYSQL_TYPE_GEOMETRY);
#if MYSQL_VERSION_ID >= 50000
    DEF(MYSQL_TYPE_BIT);
#endif

#undef DEF

#define PROXY_VERSION CHASSIS_VERSION_TAG
    lua_pushstring(L, PROXY_VERSION);
    lua_setfield(L, -2, "PROXY_VERSION");

    /**
     * create 
     * - proxy.global 
     * - proxy.global.config
     */
    lua_newtable(L);
    lua_newtable(L);

    lua_setfield(L, -2, "config");
    lua_setfield(L, -2, "global");

    lua_setglobal(L, "proxy");
}

/**
 * handle the proxy.response.* table from the lua script
 *
 * proxy.response
 *   .type can be either ERR, OK or RAW
 *   .resultset (in case of OK)
 *     .fields
 *     .rows
 *   .errmsg (in case of ERR)
 *   .packet (in case of nil)
 *
 */
int network_mysqld_con_lua_handle_proxy_response(network_mysqld_con *con, const gchar *lua_script) {
    network_mysqld_con_lua_t *st = con->plugin_con_state;
    int resp_type = 1;
    const char *str;
    size_t str_len;
    lua_State *L = st->L;

    /**
     * on the stack should be the fenv of our function */
    g_assert(lua_istable(L, -1));
    
    lua_getfield(L, -1, "proxy"); /* proxy.* from the env  */
    g_assert(lua_istable(L, -1));

    lua_getfield(L, -1, "response"); /* proxy.response */
    if (lua_isnil(L, -1)) {
        g_log_dbproxy(g_critical, "proxy.response isn't set in %s", lua_script);

        lua_pop(L, 2); /* proxy + nil */

        return -1;
    } else if (!lua_istable(L, -1)) {
        g_log_dbproxy(g_critical, "proxy.response has to be a table, is %s in %s",
                lua_typename(L, lua_type(L, -1)),
                lua_script);

        lua_pop(L, 2); /* proxy + response */
        return -1;
    }

    lua_getfield(L, -1, "type"); /* proxy.response.type */
    if (lua_isnil(L, -1)) {
        /**
         * nil is fine, we expect to get a raw packet in that case
         */
        g_log_dbproxy(g_critical, "proxy.response.type isn't set in %s", lua_script);

        lua_pop(L, 3); /* proxy + nil */

        return -1;

    } else if (!lua_isnumber(L, -1)) {
        g_log_dbproxy(g_critical, "proxy.response.type has to be a number, is %s in %s",
                lua_typename(L, lua_type(L, -1)),
                lua_script);
        
        lua_pop(L, 3); /* proxy + response + type */

        return -1;
    } else {
        resp_type = lua_tonumber(L, -1);
    }
    lua_pop(L, 1);

    switch(resp_type) {
    case MYSQLD_PACKET_OK: {
        GPtrArray *fields = NULL;
        GPtrArray *rows = NULL;
        gsize field_count = 0;

        lua_getfield(L, -1, "resultset"); /* proxy.response.resultset */
        if (lua_istable(L, -1)) {
            guint i;
            lua_getfield(L, -1, "fields"); /* proxy.response.resultset.fields */
            g_assert(lua_istable(L, -1));

            fields = network_mysqld_proto_fielddefs_new();
        
            for (i = 1, field_count = 0; ; i++, field_count++) {
                lua_rawgeti(L, -1, i);
                
                if (lua_istable(L, -1)) { /** proxy.response.resultset.fields[i] */
                    MYSQL_FIELD *field;
    
                    field = network_mysqld_proto_fielddef_new();
    
                    lua_getfield(L, -1, "name"); /* proxy.response.resultset.fields[].name */
    
                    if (!lua_isstring(L, -1)) {
                        field->name = g_strdup("no-field-name");
    
                        g_log_dbproxy(g_warning, "proxy.response.type = OK, "
                                "but proxy.response.resultset.fields[%u].name is not a string (is %s), "
                                "using default", 
                                i,
                                lua_typename(L, lua_type(L, -1)));
                    } else {
                        field->name = g_strdup(lua_tostring(L, -1));
                    }
                    lua_pop(L, 1);
    
                    lua_getfield(L, -1, "type"); /* proxy.response.resultset.fields[].type */
                    if (!lua_isnumber(L, -1)) {
                        g_log_dbproxy(g_warning, "proxy.response.type = OK, "
                                "but proxy.response.resultset.fields[%u].type is not a integer (is %s), "
                                "using MYSQL_TYPE_STRING", 
                                i,
                                lua_typename(L, lua_type(L, -1)));
    
                        field->type = MYSQL_TYPE_STRING;
                    } else {
                        field->type = lua_tonumber(L, -1);
                    }
                    lua_pop(L, 1);
                    field->flags = PRI_KEY_FLAG;
                    field->length = 32;
                    g_ptr_array_add(fields, field);
                    
                    lua_pop(L, 1); /* pop key + value */
                } else if (lua_isnil(L, -1)) {
                    lua_pop(L, 1); /* pop the nil and leave the loop */
                    break;
                } else {
                    g_log_dbproxy(g_error, "proxy.response.resultset.fields[%d] should be a table, but is a %s", 
                            i,
                            lua_typename(L, lua_type(L, -1)));
                }
            }
            lua_pop(L, 1);
    
            rows = g_ptr_array_new();
            lua_getfield(L, -1, "rows"); /* proxy.response.resultset.rows */
            g_assert(lua_istable(L, -1));
            for (i = 1; ; i++) {
                lua_rawgeti(L, -1, i);
    
                if (lua_istable(L, -1)) { /** proxy.response.resultset.rows[i] */
                    GPtrArray *row;
                    gsize j;
    
                    row = g_ptr_array_new();
    
                    /* we should have as many columns as we had fields */
        
                    for (j = 1; j < field_count + 1; j++) {
                        lua_rawgeti(L, -1, j);
    
                        if (lua_isnil(L, -1)) {
                            g_ptr_array_add(row, NULL);
                        } else {
                            g_ptr_array_add(row, g_strdup(lua_tostring(L, -1)));
                        }
    
                        lua_pop(L, 1);
                    }
    
                    g_ptr_array_add(rows, row);
    
                    lua_pop(L, 1); /* pop value */
                } else if (lua_isnil(L, -1)) {
                    lua_pop(L, 1); /* pop the nil and leave the loop */
                    break;
                } else {
                    g_log_dbproxy(g_error, "proxy.response.resultset.rows[%d] should be a table, but is a %s", 
                            i,
                            lua_typename(L, lua_type(L, -1)));
                }
            }
            lua_pop(L, 1);

            network_mysqld_con_send_resultset(con->client, fields, rows);
        } else {
            guint64 affected_rows = 0;
            guint64 insert_id = 0;

            lua_getfield(L, -2, "affected_rows"); /* proxy.response.affected_rows */
            if (lua_isnumber(L, -1)) {
                affected_rows = lua_tonumber(L, -1);
            }
            lua_pop(L, 1);

            lua_getfield(L, -2, "insert_id"); /* proxy.response.affected_rows */
            if (lua_isnumber(L, -1)) {
                insert_id = lua_tonumber(L, -1);
            }
            lua_pop(L, 1);

            network_mysqld_con_send_ok_full(con->client, affected_rows, insert_id, 0x0002, 0);
        }

        /**
         * someone should cleanup 
         */
        if (fields) {
            network_mysqld_proto_fielddefs_free(fields);
            fields = NULL;
        }

        if (rows) {
            guint i;
            for (i = 0; i < rows->len; i++) {
                GPtrArray *row = rows->pdata[i];
                guint j;

                for (j = 0; j < row->len; j++) {
                    if (row->pdata[j]) g_free(row->pdata[j]);
                }

                g_ptr_array_free(row, TRUE);
            }
            g_ptr_array_free(rows, TRUE);
            rows = NULL;
        }

        
        lua_pop(L, 1); /* .resultset */
        
        break; }
    case MYSQLD_PACKET_ERR: {
        gint errorcode = ER_UNKNOWN_ERROR;
        gboolean nologerr = FALSE;
        const gchar *sqlstate = "07000"; /** let's call ourself Dynamic SQL ... 07000 is "dynamic SQL error" */
        
        lua_getfield(L, -1, "errcode"); /* proxy.response.errcode */
        if (lua_isnumber(L, -1)) {
            errorcode = lua_tonumber(L, -1);
        }
        lua_pop(L, 1);

        lua_getfield(L, -1, "sqlstate"); /* proxy.response.sqlstate */
        sqlstate = lua_tostring(L, -1);
        lua_pop(L, 1);

        lua_getfield(L, -1, "nologerr"); /* proxy.response.nologerr */
        if (lua_isnumber(L, -1)) {
            nologerr = lua_tonumber(L, -1);
        }
        lua_pop(L, 1);

        lua_getfield(L, -1, "errmsg"); /* proxy.response.errmsg */
        if (lua_isstring(L, -1)) {
            str = lua_tolstring(L, -1, &str_len);
            network_mysqld_con_send_error_full_nolog(con->client, str, str_len, errorcode, sqlstate);
            if (!nologerr) {
                SEND_ERR_MSG_HANDLE(g_critical, str, con->client);
            }
        } else {
                network_mysqld_con_send_error_full_nolog(con->client, C("(lua) proxy.response.errmsg is nil"), errorcode, NULL);
                if (!nologerr) {
                    SEND_ERR_MSG_HANDLE(g_critical, "(lua) proxy.response.errmsg is nil", con->client);
                }
        }
        lua_pop(L, 1);

        break; }
    case MYSQLD_PACKET_RAW: {
        guint i;
        /**
         * iterate over the packet table and add each packet to the send-queue
         */
        lua_getfield(L, -1, "packets"); /* proxy.response.packets */
        if (lua_isnil(L, -1)) {
            g_log_dbproxy(g_critical, "proxy.response.packets isn't set in %s",
                    lua_script);

            lua_pop(L, 2 + 1); /* proxy + response + nil */

            return -1;
        } else if (!lua_istable(L, -1)) {
            g_log_dbproxy(g_critical, "proxy.response.packets has to be a table, is %s in %s",
                    lua_typename(L, lua_type(L, -1)),
                    lua_script);

            lua_pop(L, 2 + 1); /* proxy + response + packets */
            return -1;
        }

        for (i = 1; ; i++) {
            lua_rawgeti(L, -1, i);

            if (lua_isstring(L, -1)) { /** proxy.response.packets[i] */
                str = lua_tolstring(L, -1, &str_len);

                network_mysqld_queue_append(con->client, con->client->send_queue,
                        str, str_len);
    
                lua_pop(L, 1); /* pop value */
            } else if (lua_isnil(L, -1)) {
                lua_pop(L, 1); /* pop the nil and leave the loop */
                break;
            } else {
                g_log_dbproxy(g_error, "proxy.response.packets should be array of strings, field %u was %s", 
                        i,
                        lua_typename(L, lua_type(L, -1)));
            }
        }

        lua_pop(L, 1); /* .packets */

        network_mysqld_queue_reset(con->client); /* reset the packet-id checks */

        break; }
    default:
        g_log_dbproxy(g_critical, "proxy.response.type is unknown: %d", resp_type);

        lua_pop(L, 2); /* proxy + response */

        return -1;
    }

    lua_pop(L, 2);
    return 0;
}

static int
add_blacklists_item(chassis *chas, const char *sql_raw, int inflag)
{
    sql_filter      *cur_filter = NULL;
    gchar           *sql_rewrite_md5 = NULL;
    GString         *sql_rewrite = NULL;
    GPtrArray       *tokens = NULL;

    g_assert(sql_raw != NULL);

    tokens = sql_tokens_new();
    sql_tokenizer(tokens, C_S(sql_raw));
    sql_rewrite = sql_filter_sql_rewrite(tokens);
    sql_tokens_free(tokens);

    if (sql_rewrite == NULL)
    {
       g_log_dbproxy(g_warning, "[filter][add blacklist][failed], raw sql = %s", sql_raw);
       return 1;
    }

    /* sql_rewrite .... */
    sql_rewrite_md5 = g_compute_checksum_for_string(G_CHECKSUM_MD5,
                                                              S(sql_rewrite));

    cur_filter = chas->proxy_filter;
    if (cur_filter != NULL)
    {
        int flag = ((inflag == -1) ? cur_filter->manual_filter_flag : inflag);
        sql_filter_hval *hval = NULL;

        g_rw_lock_writer_lock(&cur_filter->sql_filter_lock);
        hval = sql_filter_lookup(cur_filter, sql_rewrite_md5);
        if (hval == NULL)
        {
            sql_filter_insert(cur_filter, sql_rewrite->str,
                                sql_rewrite_md5, flag, MANUAL_ADD_FILTER);
        } else {
            hval->flag = flag;
        }
        g_rw_lock_writer_unlock(&cur_filter->sql_filter_lock);

        g_log_dbproxy(g_message, "[filter][add blacklist][success][flag = %d] [filter: %s] [hashcode: %s]",
                        flag, sql_rewrite->str, sql_rewrite_md5);
    }

    g_free(sql_rewrite_md5);
    g_string_free(sql_rewrite, TRUE);

    return 0;
}

static int
update_blacklists_item(chassis *chas, const char *hash_code, int flag)
{
    g_assert(hash_code != NULL);

    if (chas->proxy_filter != NULL)
    {
        sql_filter *up_filter = chas->proxy_filter;
        g_rw_lock_writer_lock(&up_filter->sql_filter_lock);
        sql_filter_hval *hval = sql_filter_lookup(up_filter, hash_code);
        if (hval != NULL) { hval->flag = flag; }
        g_rw_lock_writer_unlock(&up_filter->sql_filter_lock);
    }

    g_log_dbproxy(g_message, "[filter][update blacklist][success][flag = %d][hashcode: %s]", flag, hash_code);
    return 0;
}

static int
remove_blacklists_item(chassis *chas, const char *hash_code)
{
    g_assert(hash_code != NULL && chas != NULL);

    if (chas->proxy_filter == NULL)
    {
        g_log_dbproxy(g_warning, "[filter][remove blacklist][not exist][hashcode: %s]", hash_code);
        return 0;
    }

    g_rw_lock_writer_lock(&chas->proxy_filter->sql_filter_lock);
    sql_filter_remove(chas->proxy_filter, hash_code);
    g_rw_lock_writer_unlock(&chas->proxy_filter->sql_filter_lock);

    g_log_dbproxy(g_message, "[filter][remove blacklist][success][hashcode: %s]", hash_code);

    return 0;
}

static int
clear_blacklists(chassis *chas)
{
    if (chas->proxy_filter != NULL)
    {
        GHashTableIter iter;
        sql_filter_hval *value;
        gchar       *key;

        g_rw_lock_writer_lock(&chas->proxy_filter->sql_filter_lock);
        g_hash_table_iter_init (&iter, chas->proxy_filter->blacklist);
        while (g_hash_table_iter_next (&iter, (gpointer)&key, (gpointer)&value))
        {
            g_hash_table_iter_remove(&iter);
        }
        g_rw_lock_writer_unlock(&chas->proxy_filter->sql_filter_lock);
    }

    g_log_dbproxy(g_message, "[filter][clear blacklist][success]");
    return 0;
}

static int
save_blacklists(sql_filter *cur_filter)
{
    GHashTableIter  iter;
    sql_filter_hval *value = NULL;
    gchar           *key = NULL;
    int i = 0;
    int ret = 1;

    gchar *tmp_name = g_strdup_printf("%s.tmp", cur_filter->blacklist_file);

    if (access(tmp_name, F_OK)) {  unlink(tmp_name);  }

    FILE *fp = fopen(tmp_name, "a+");
    if (fp == NULL)
    {
        g_log_dbproxy(g_warning, "[filter][save][open temp file failed][%s]", tmp_name);
        g_free(tmp_name);
        return ret;
    }

    g_rw_lock_reader_lock(&cur_filter->sql_filter_lock);
    g_hash_table_iter_init (&iter, cur_filter->blacklist);
    while (g_hash_table_iter_next (&iter, (gpointer)&key, (gpointer)&value))
    {
        /* write file */
        GKeyFile* config = g_key_file_new();
        gchar *filter_name = g_strdup_printf("blacklist_%d", i);
        gchar *content = NULL;
        gsize length = 0;

        g_key_file_set_value(config, filter_name, "filter", value->sql_filter_item);
        g_key_file_set_integer(config, filter_name, "is_enabled", value->flag);
        g_key_file_set_integer(config, filter_name, "filter_status", value->filter_status);

        content = g_key_file_to_data(config, &length, NULL);
        if (fwrite(content, length, 1, fp) != 1)
        {
            g_log_dbproxy(g_warning, "[filter][write file][failed]:%s", content);
            g_free(filter_name);
            g_free(content);
            g_key_file_free(config);
            break;
        }

        g_free(filter_name);
        g_free(content);
        g_key_file_free(config);
        i++;
    }
    g_rw_lock_reader_unlock(&cur_filter->sql_filter_lock);

    fclose(fp);

    if (g_hash_table_size(cur_filter->blacklist) == i) {
    if (0 == access(cur_filter->blacklist_file, 0)) {//check whether the blacklist-file is exist
            if (unlink(cur_filter->blacklist_file) != 0)//if the blacklist-file is exist, then unlink the file
            {
                g_warning("[filter][save][unlink old file failed]: [%s]", g_strerror(errno));
            if (unlink(tmp_name) != 0) {//if unlink the old file failed, then unlink the temporary file
                g_log_dbproxy(g_warning, "[filter][save][unlink temporary file failed]: [%s]", g_strerror(errno));
            }
                return ret;
            }
    }
    if (rename(tmp_name, cur_filter->blacklist_file) != 0) {
        g_warning("[filter][save][rename failed][%s]", g_strerror(errno));
        if (unlink(tmp_name) != 0) {
            g_warning("[filter][save][unlink temporary file failed]: [%s]", g_strerror(errno));
        }
            return ret;
        }

        g_log_dbproxy(g_message, "[filter][save blacklist][success][%s]", cur_filter->blacklist_file);
        ret = 0;
    }
    return ret;
}

static gboolean
try_get_int64_value(const gchar *option_value, gint64 *return_value)
{
    gchar *endptr = NULL;
    gint64 value = 0;
    g_assert(option_value != NULL);

    value = strtoll(option_value, &endptr, 0);
    if ((endptr != NULL && *endptr != '\0') ||
            (errno == ERANGE && (value == G_MAXINT64 || value == G_MININT64)) ||
            endptr == option_value)
        return FALSE;

    *return_value = value;
    return TRUE;
}
static gboolean
try_get_double_value(const gchar *option_value, gdouble *return_value)
{
    gchar *endptr = NULL;
    gdouble value = 0.0;
    gdouble adjust_value = 0.0;

    g_assert(option_value != NULL);

    value = strtod(option_value, &endptr);
    if ((endptr != NULL && *endptr != '\0') ||
            (errno == ERANGE && (value == G_MAXFLOAT || value == G_MINFLOAT)) ||
            endptr == option_value)
        return FALSE;

    *return_value = value;
    return TRUE;
}
static gchar *
trim_str(const gchar *str)
{
    gchar *trim_str = NULL;
    gint len = 0;

    g_assert(str != NULL);

    len = strlen(str);

    if (str[0] != '\'' || str[len-2] != '\'') {
        return NULL;
    }

    trim_str = (gchar *)g_malloc0(len - 1);
    g_strlcpy(trim_str, str+1, len - 2);

    return trim_str;
}


static int
proxy_sys_config_call(lua_State *L)
{
    gsize keysize = 0;

    chassis *chas = *(chassis **)luaL_checkself(L);
    const gchar *key = luaL_checklstring(L, -1, &keysize);
    const gchar *option_value = luaL_checkstring(L, -2);

    gint ret = 1;

    if (strleq(key, keysize, C("saveconfig"))) {
        ret = save_config(chas);
    } else if (strleq(key, keysize, C("show_variables"))) {
        show_variables(chas, L, option_value);
        return 1;
    } else if (strleq(key, keysize, C("show_tables"))) {
        tables_opts_handle(chas, SHOW_SHARD_TABLE, L, option_value);
        return 1;
    } else if (strleq(key, keysize, C("add_tables"))) {
        ret = tables_opts_handle(chas, ADD_SHARD_TABLE, L, option_value);
    } else if (strleq(key, keysize, C("remove_tables"))) {
        ret = tables_opts_handle(chas, RM_SHARD_TABLE, L, option_value);
    } else if (strleq(key, keysize, C("addblacklist"))) {
        const char *flag = luaL_checkstring(L, -3);
        gint64 value = -1;

        if (strlen(flag) == 0) {
            ret = 0;
        } else if (try_get_int64_value(flag, &value)) {
            ret = ((value == 0 || value == 1) ? 0 : 1);
        }

        if (ret != 1) { ret = add_blacklists_item(chas, option_value, value); }
    } else if (strleq(key, keysize, C("updateblacklist"))) {
        const char *flag = luaL_checkstring(L, -3);
        gint64 value = -1;

        g_assert(flag != NULL);

        if (try_get_int64_value(flag, &value) && (value == 0 || value == 1)) {
            ret = update_blacklists_item(chas, option_value, value);
    }
    } else if (strleq(key, keysize, C("removeblacklist"))) {
        ret = remove_blacklists_item(chas, option_value);
    } else if (strleq(key, keysize, C("clearblacklist"))) {
        ret = clear_blacklists(chas);
    } else if (strleq(key, keysize, C("saveblacklist"))) {
        ret = save_blacklists(chas->proxy_filter);
    } else if (strleq(key, keysize, C("loadblacklist"))) {
        ret = load_sql_filter_from_file(chas->proxy_filter);
    } else if (strleq(key, keysize, C("shutdown"))) {
        gchar *msg = NULL;
        gint   shutdown_mode = CHAS_SHUTDOWN_NORMAL;
        if (strcasecmp(option_value, "NORMAL") == 0 || strlen(option_value) == 0) {
            msg = "admin shutdown normal";
            shutdown_mode = CHAS_SHUTDOWN_NORMAL;
            ret = 0;
        } else if (strcasecmp(option_value, "IMMEDIATE") == 0) {
            msg = "admin shutdown immediate";
            shutdown_mode = CHAS_SHUTDOWN_IMMEDIATE;
            ret = 0;
        }
        if (ret == 0) { chassis_set_shutdown_location(msg, shutdown_mode); }
    } else if (strleq(key, keysize, C("kill"))) {
        if (strlen(option_value) != 0) {
            gint64 con_id = 0;
            if (try_get_int64_value(option_value, &con_id)) {
                ret = kill_one_connection(chas, con_id);
            }

            if (ret == 1) {
                g_log_dbproxy(g_warning, "kill invalid connection id %lu in event_thread %lu",
                                                    con_id, GET_THEAD_ID(con_id));
            }
        }
    } else if (strleq(key, keysize, C("show_percentile"))) {
        chassis_option_t    *opt = NULL;
        external_param      *st_param = g_new0(external_param, 1);
        gchar               *percentil = NULL;

        opt = find_option_from_chas(chas, "percentile");
        st_param->magic_value = (void*) option_value;
        percentil = opt->show_hook((void *)st_param);
        lua_pushstring(L, percentil);
        g_free(percentil);
        g_free(st_param);

        return 1;
    } else {
        /* set key/value options */
        chassis_option_t *opt = NULL;

        opt = find_option_from_chas(chas, key);
        if (opt != NULL && opt->assign_opts_hook != NULL &&
                            CAN_ASSGIN_OPTS(opt->opt_property)) {
            external_param *st_param = g_new0(external_param, 1);

            st_param->chas = chas;
            ret = opt->assign_opts_hook(option_value, (void *)st_param);
            g_free(st_param);
        } else {
            ret = 2;
    }
    }

    lua_pushinteger(L, ret);

    return 1;
}

int network_sys_config_lua_getmetatable(lua_State *L)
{
    static const struct luaL_reg methods[] = {
            { "__call", proxy_sys_config_call },
            { NULL, NULL }
    };

    return proxy_getmetatable(L, methods);
}

static void
show_reserved_query(GQueue *gl_reserved_query, lua_State *L, int begin_idx)
{
    gint i = 0;
    gint len = 0;
    gint query_num = begin_idx;

    if (gl_reserved_query == NULL) return ;

    len = g_queue_get_length(gl_reserved_query);
    if (len == 0) return ;

    for(i = 0; i < len; i++)
    {
        reserved_query_item *rqi = g_queue_peek_nth (gl_reserved_query, i);
        struct tm cur_tm;
        GString *time_str = g_string_new(NULL);
#if 0
        if (flag == 1 && (rqi->item_status & ~RQ_HIT_BY_FILTER) &&
                           (rqi->item_status & ~RQ_FOBIDDEN_BY_FILTER))
            continue;
#endif

        lua_pushnumber(L, query_num);
        lua_newtable(L);

        lua_pushstring(L, "query_rewrite");
        lua_pushlstring(L, S(rqi->item_rewrite));
        lua_settable(L,-3);

        lua_pushstring(L, "fist_access_time");
        localtime_r(&rqi->item_first_access_time, &cur_tm);
        g_string_printf(time_str, "%02d/%02d/%d %02d:%02d:%02d",
                                cur_tm.tm_mon+1, cur_tm.tm_mday, cur_tm.tm_year+1900,
                                cur_tm.tm_hour, cur_tm.tm_min, cur_tm.tm_sec);
        lua_pushlstring(L, S(time_str));
        lua_settable(L,-3);

        lua_pushstring(L, "last_access_time");
        localtime_r(&rqi->item_last_access_time, &cur_tm);
        g_string_truncate(time_str, 0);
        g_string_printf(time_str, "%02d/%02d/%d %02d:%02d:%02d",
                                                    cur_tm.tm_mon+1, cur_tm.tm_mday, cur_tm.tm_year+1900,
                                                    cur_tm.tm_hour, cur_tm.tm_min, cur_tm.tm_sec);
        lua_pushlstring(L, S(time_str));
        lua_settable(L,-3);

        lua_pushstring(L, "query_times");
        lua_pushnumber(L, rqi->item_access_num);
        lua_settable(L,-3);

        lua_pushstring(L, "lastest_query_times");
        lua_pushnumber(L, rqi->item_gap_access_num);
        lua_settable(L,-3);

        lua_settable(L, -3);
        g_string_free(time_str, TRUE);
        query_num++;
    }
}

static void
send_options_to_lua(chassis_options_t *opts, const char *group_name, lua_State *L,
                                            chassis *chas, const char *var_name)
{
    GList *node;

    g_assert(opts != NULL);

    for (node = opts->options; node; node = node->next) {
        chassis_option_t *opt = node->data;

        if (CAN_SHOW_OPTS(opt->opt_property) && opt->show_hook != NULL) {
            external_param  *opt_param = g_new0(external_param, 1);
            gchar           *value = NULL;

            if (strlen(var_name) != 0) {
                if (!opt_match(opt->long_name, var_name)) {
                    continue;
                }
            }

            lua_pushstring(L, opt->long_name);
            lua_newtable(L);

            lua_pushstring(L, "group");
            lua_pushlstring(L, C_S(group_name));
            lua_settable(L, -3);

            lua_pushstring(L, "value");
            opt_param->chas = chas;
            opt_param->opt_type = SHOW_OPTS;
            value = opt->show_hook((void *)opt_param);
            if (value != NULL) {
                lua_pushlstring(L, C_S(value));
            } else {
                lua_pushlstring(L, C_S(""));
            }
            lua_settable(L, -3);

            lua_pushstring(L, "set_mode");
            if (CAN_ASSGIN_OPTS(opt->opt_property)) {
                lua_pushlstring(L, C("Dynamic"));
            } else {
                lua_pushlstring(L, C("Static"));
            }
            lua_settable(L, -3);

            lua_settable(L, -3);
            if (value) g_free(value);
            if (opt_param) g_free(opt_param);
        }
    }
}

static chassis_option_t *
find_option_by_name(const gchar *opt_name, chassis_options_t *opts)
{
    GList *node = NULL;

    g_assert(opt_name != NULL && opts != NULL);

    for (node = opts->options; node; node = node->next) {
        chassis_option_t *opt = node->data;
        if (strcmp(opt_name, opt->long_name) == 0) {
            return opt;
        }
    }

    return NULL;
}

static chassis_option_t *
find_option_from_chas(chassis *chas, const gchar *opt_name)
{
    chassis_option_t *opt =  NULL;
    gint i = 0;

    g_assert(chas != NULL && opt_name != NULL);

    /* find in main */
    opt = find_option_by_name(opt_name, chas->opts);

    /* find in plugins */
    if (opt == NULL) {
        for (i = 0; i < chas->modules->len; i++) {
            chassis_options_t *plugin_opts = NULL;
            chassis_plugin *p = chas->modules->pdata[i];
            if (NULL != (plugin_opts = chassis_plugin_get_options(p))) {
                opt = find_option_by_name(opt_name, plugin_opts);
                if (opt != NULL) {
                    break;
                }
            }
        }
    }

    return opt;
}

static gint
tables_opts_handle(chassis *chas, gint tables_opts_type, lua_State *L, const char *option_value)
{
    chassis_option_t *opt = NULL;
    chassis_options_t *plugin_opts = NULL;
    chassis_plugin *p = chas->modules->pdata[1];
    gint ret = 1;

    external_param *st_param = g_new0(external_param, 1);
    if (NULL != (plugin_opts = chassis_plugin_get_options(p))) {
        opt = find_option_by_name("tables", plugin_opts);
    }

    st_param->L = (void *) L;
    st_param->tables = option_value;
    st_param->opt_type = tables_opts_type;
    if (tables_opts_type == SAVE_SHARD_TABLE || tables_opts_type == SHOW_SHARD_TABLE) {
        opt->show_hook((void *)st_param);
        ret = 0;
    } else if (tables_opts_type == RM_SHARD_TABLE || tables_opts_type == ADD_SHARD_TABLE) {
        ret = opt->assign_opts_hook(option_value, (void *)st_param);
    }
    g_free(st_param);

    return ret;
}

static gint
show_variables(chassis *chas, lua_State *L, const char *option_value)
{
    gint i = 0;

    g_assert(chas != NULL && L != NULL);

    lua_newtable(L);

    if (chas->opts != NULL) {
        send_options_to_lua(chas->opts, "main", L, chas, option_value);
    }

    for (i = 0; i < chas->modules->len; i++) {
        chassis_options_t *plugin_opts = NULL;
        chassis_plugin *p = chas->modules->pdata[i];
        if (NULL != (plugin_opts = chassis_plugin_get_options(p))) {
            send_options_to_lua(plugin_opts, p->option_grp_name,
                                L, chas, option_value);
        }
    }

    return 0;
}
