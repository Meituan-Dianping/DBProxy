/* Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file Representation of an SQL command.
*/

#ifndef NETWORK_MYSQL_STATS_INCLUDED
#define NETWORK_MYSQL_STATS_INCLUDED

#include <glib.h>
#include <math.h>


#define SET_MIN(min, cur) \
    if((min) == 0 || (cur) < (min)) {(min) = (cur);}
#define SET_MAX(max, cur) \
    if((cur) > (max)) {(max) = (cur);}

typedef enum {
    COM_QUERY_READ = 0,
    COM_QUERY_WRITE,
    COM_OTHER,
    QUERY_TYPE_NUM
} query_type;

typedef enum {
    THREAD_STAT_COM_QUERY_READ = 0,
    THREAD_STAT_COM_QUERY_WRITE = 1,
    THREAD_STAT_COM_OTHER = 2,
    THREAD_STAT_COM_QUERY_SEND_READ,
    THREAD_STAT_COM_QUERY_SEND_WRITE,
    THREAD_STAT_COM_OTHER_SEND,
    THREAD_STAT_SLOW_QUERY,
    THREAD_STAT_SLOW_QUERY_RT,
    THREAD_STAT_SERVER_READ,
    THREAD_STAT_SERVER_WRITE,
    THREAD_STAT_CLIENT_READ ,
    THREAD_STAT_CLIENT_WRITE,
    THREAD_STAT_SERVER_READ_PKT,
    THREAD_STAT_SERVER_WRITE_PKT,
    THREAD_STAT_CLIENT_READ_PKT,
    THREAD_STAT_CLIENT_WRITE_PKT,
    THREAD_STAT_EVENT_WAITING,
    THREAD_STAT_THREADS_RUNNING,
    THREAD_STAT_END,
    GLOBAL_STAT_MAX_CONNECTIONS,
    GLOBAL_STAT_MAX_USED_CONNECTIONS,
    GLOBAL_STAT_CUR_CONNECTIONS,
    GLOBAL_STAT_DB_CONNECTIONS,
    GLOBAL_STAT_DB_CONNECTIONS_CACHED,
    GLOBAL_STAT_ATTEMPED_CONNECTS,
    GLOBAL_STAT_ABORTED_CONNECTS,
    GLOBAL_STAT_CLOSED_CLIENTS,
    GLOBAL_STAT_ABORTED_CLIENTS,
    GLOBAL_STAT_END,
    PERCENTILE, 
    PROXY_STAT_END
} network_mysqld_stat_type_t;

#define MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS 12

// event thread执行过程中同步方式的等待，不包括event loop的等待
typedef enum {
    WAIT_EVENT_SERVER_CONNECT = 0,
    WAIT_EVENT_END
} network_mysqld_wait_event_t;

typedef struct wait_event_status_t
{
    network_mysqld_wait_event_t event_type; // 当前的等待事件类型
    gint64 wait_start;                      // 当前等待的开始时间
} wait_event_status_t;

typedef struct wait_event_stat_t
{
    guint64 waits;                  // 总等待次数
    guint64 longer_waits;           // 等待时间超过阈值的等待次数
    guint64 min_wait_time;          // 最短的等待时间
    guint64 max_wait_time;          // 最长的等待时间
    guint64 total_wait_time;        // 总的等待时间
    guint64 total_longer_wait_time; // 所有超过阈值的等待时间
} wait_event_stat_t;

typedef struct response_time_stat_t
{
    guint64 query_num;
    guint64 min_respon_time;
    guint64 max_respon_time;
    guint64 total_respon_time;
} response_time_stat_t;

typedef struct thread_status_var_t
{
    guint64 thread_stat[THREAD_STAT_END];
    wait_event_status_t wait_event_status;
    wait_event_stat_t wait_event_stat[WAIT_EVENT_END];

    response_time_stat_t all_query_rt_stat[QUERY_TYPE_NUM];
    response_time_stat_t hist_query_rt_stat[QUERY_TYPE_NUM][MAX_QUEYR_RESPONSE_TIME_HIST_LEVELS];
} thread_status_var_t;

// 连接等待状态，同时包括同步和异步的等待
typedef enum {
    CON_NO_WAIT = 0,
    CON_WAIT_CLIENT_READ = 1,
    CON_WAIT_CLIENT_WRITE = 2,
    CON_WAIT_SERVER_READ = 3,
    CON_WAIT_SERVER_WRITE = 4,
    CON_WAIT_SERVER_CONNECT = 5,
    CON_WAIT_END
} network_mysqld_con_wait_t;


#define STMT_LENTH 1024
typedef struct connection_status_var_t
{
    guint64 cur_query_type;
    guint64 cur_query_start_time;
    gboolean query_running;
    gchar cur_query[STMT_LENTH];
    guint64 cur_query_com_type;
} connection_status_var_t;

extern const char *network_mysqld_stat_desc[];
extern const char *network_mysqld_conn_wait_desc[];
extern const char *network_mysqld_conn_stat_desc[];

extern const char *chas_thread_wait_event_desc[];

extern const char *network_mysqld_query_type_desc[];

#endif // NETWORK_MYSQL_STATS_INCLUDED
