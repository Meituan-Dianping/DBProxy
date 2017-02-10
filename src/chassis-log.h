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
 

#ifndef _CHASSIS_LOG_H_
#define _CHASSIS_LOG_H_

#include <glib.h>
#ifdef _WIN32
#include <windows.h>
#endif

#include "chassis-exports.h"

#define CHASSIS_RESOLUTION_SEC  0x0
#define CHASSIS_RESOLUTION_MS   0x1
#define CHASSIS_RESOLUTION_US   0x2

#define CHASSIS_RESOLUTION_DEFAULT  CHASSIS_RESOLUTION_SEC


#define DEBUG_TRACE_DISABLE         0x00
#define DEBUG_TRACE_CONNECTION_POOL 0x01
#define DEBUG_TRACE_WAIT_EVENT      0x02
#define DEBUG_TRACE_SQL             0x04
#define DEBUG_TRACE_CON_STATUS      0x08
#define DEBUG_TRACE_SHARD           0x10
#define DEBUG_TRACE_ALL             (DEBUG_TRACE_CONNECTION_POOL | \
                                        DEBUG_TRACE_WAIT_EVENT | \
                                        DEBUG_TRACE_SQL | \
                                        DEBUG_TRACE_CON_STATUS | \
                                        DEBUG_TRACE_SHARD)


#define TRACE_CONNECTION_POOL(mode) ((mode) & DEBUG_TRACE_CONNECTION_POOL)
#define TRACE_WAIT_EVENT(mode)      ((mode) & DEBUG_TRACE_WAIT_EVENT)
#define TRACE_SQL(mode)             ((mode) & DEBUG_TRACE_SQL)
#define TRACE_CON_STATUS(mode)      ((mode) & DEBUG_TRACE_CON_STATUS)
#define TRACE_SHARD(mode)           ((mode) & DEBUG_TRACE_SHARD)

#define SQL_LOG_DIR         "sql"

/** @addtogroup chassis */
/*@{*/
typedef struct {
    GLogLevelFlags min_lvl;

    gchar *log_filename;
    gint log_file_fd;

    gboolean use_syslog;

#ifdef _WIN32
    HANDLE event_source_handle;
    gboolean use_windows_applog;
#endif
    gboolean rotate_logs;

    GString *log_ts_str;
    gint     log_ts_resolution; /*<< timestamp resolution (sec, ms) */

    GString *last_msg;
    time_t   last_msg_ts;
    guint    last_msg_count;

    gint   log_trace_modules;

    GMutex log_mutex;
} chassis_log;

typedef struct {
    char *name;
    GLogLevelFlags lvl;
    int syslog_lvl;
    int win_evtype;
} log_lvl;

#define LOG_LVL_MAP_SIZE    6
extern const log_lvl log_lvl_map[];

CHASSIS_API chassis_log *chassis_log_init(void) G_GNUC_DEPRECATED;
CHASSIS_API chassis_log *chassis_log_new(void);
CHASSIS_API int chassis_log_set_level(chassis_log *log, const gchar *level);
CHASSIS_API void chassis_log_free(chassis_log *log);
CHASSIS_API int chassis_log_open(chassis_log *log);
CHASSIS_API void chassis_log_func(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
CHASSIS_API void chassis_log_set_logrotate(chassis_log *log);
CHASSIS_API int chassis_log_set_event_log(chassis_log *log, const char *app_name);
CHASSIS_API const char *chassis_log_skip_topsrcdir(const char *message);
CHASSIS_API void chassis_set_logtimestamp_resolution(chassis_log *log, int res);
CHASSIS_API int chassis_get_logtimestamp_resolution(chassis_log *log);
CHASSIS_API int chassis_log_update_timestamp(GString *log, guint log_ts_resolution);
CHASSIS_API GString* chassis_log_microsecond_tostring(guint64 micro_sec_time, guint log_ts_resolution);

#define DEFAULT_LOG_MAX_NUM          50000
#define DEFAULT_LOG_WAIT_TIMEOUT_US  500000

typedef struct log_queue {
    GQueue *log_q;
    GMutex log_q_t_lock;
    gint  log_q_max_length;
    volatile gint length_status;
    gint  log_q_wait_timeout_us;
} log_queue;

CHASSIS_API log_queue* log_queue_new();
CHASSIS_API void log_queue_free(log_queue *lq);
CHASSIS_API gint log_queue_push(log_queue *lq, GString *data);
CHASSIS_API GString* log_queue_pop();

#define g_log_dbproxy(log_level, reason, ...) log_level("%s(%s)"reason ,G_STRLOC,  __func__, ##__VA_ARGS__)

/*@}*/

#endif
