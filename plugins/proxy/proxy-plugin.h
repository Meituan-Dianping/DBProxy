/* $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _PROXY_PLUGIN_H
#define _PROXY_PLUGIN_H

#include <glib.h>
#include "network-mysqld.h"


#include "proxy-percentile.h"
#include "proxy-sql-log.h"


typedef struct plugin_thread_param {
    void    *magic_value;
    GCond   *plugin_thread_cond;
    GMutex  *plugin_thread_mutex;
} plugin_thread_param;

typedef struct plugin_thread_t {
    GThread *thr;
    GCond   thr_cond;
    GMutex  thr_mutex;
    plugin_thread_param *thread_param;
} plugin_thread_t;

typedef struct plugin_thread_info {
    gchar   *plugin_thread_names;
    void*   (*thread_fn)(void *user_data);
    void    *thread_args;
} plugin_thread_info;

typedef enum {
    SEL_OFF,
    SEL_ON
} SELECT_WHERE_LIMIT;

typedef struct tbl_name_wrap {
    gchar *prefix;
    gchar *suffix;
    GRWLock name_wrap_lock;
} tbl_name_wrap;

struct chassis_plugin_config {
    gchar *address;                   /**< listening address of the proxy */

    gchar **backend_addresses;        /**< read-write backends */
    gchar **read_only_backend_addresses; /**< read-only  backends */

    gint fix_bug_25371;               /**< suppress the second ERR packet of bug #25371 */

    gint profiling;                   /**< skips the execution of the read_query() function */

    gchar *lua_script;                /**< script to load at the start the connection */

    gint pool_change_user;            /**< don't reset the connection, when a connection is taken from the pool
                        - this safes a round-trip, but we also don't cleanup the connection
                        - another name could be "fast-pool-connect", but that's too friendly
                       */

    gint start_proxy;
    gint check_state_conn_timeout;
    gint check_state_interval;
    gint check_state_retry_times;
    gint check_state_sleep_delay;

    gchar **client_ips;
    gchar **lvs_ips;

    gchar **tables;
    GHashTable *dt_table;

    gchar **pwds;

    network_mysqld_con *listen_con;
    gchar *select_where_limit_str;
    SELECT_WHERE_LIMIT select_where_limit;

    gchar **user_ips_str;
    gchar **user_backends_str;

    gchar *charset;
    GRWLock config_lock;

    gchar *percentile_switch;
    gint    percentile_value;
    gdouble percentile;
    pt_percentile_t *percentile_controller;

    gchar *sql_log_type;
    gchar *sql_log_mode;
    sql_log_t *sql_log_mgr;

    chassis_options_t *opts;            /* save the proxy plugins options */
    GHashTable *plugin_threads;

    gchar   *table_prefix;
    gchar   *table_suffix;
    tbl_name_wrap   *tnw;
};

extern chassis_plugin_config *config;
#endif  /* _PROXY_PLUGIN_H */

