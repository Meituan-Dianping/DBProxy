#ifndef __PROXY_SQL_LOG_H__
#define __PROXY_SQL_LOG_H__

#include <stdlib.h>
#include <stdio.h>

#include "network-injection.h"
#include "network-mysqld.h"

#include "proxy-sql-log.h"

typedef enum {
    OFF,
    ON,
    REALTIME
} SQL_LOG_TYPE;

#define  SQL_LOG_CLIENT      0x01
#define  SQL_LOG_BACKEND     0x02
#define  SQL_LOG_ALL         (SQL_LOG_BACKEND | SQL_LOG_CLIENT)

#define SQL_LOG_DEFAULT_SIZE 1073741824
#define SQL_LOG_DEFAULT_NUM 0

#define PROXY_SQL_LOG_THREAD        "log_manager"

#define CHECK_LOG_MANAGER_TIMEOUT     4
#define LOG_MANAGER_BUFFER_TIMEOUT    500000

typedef struct sql_log_t {
    FILE                *sql_log_fp;
    SQL_LOG_TYPE        sql_log_type;       /* internal mode for "sql-log" */
    gint                sql_log_mode;       /* internal mode for "sql-log-mode" */
    gchar               *sql_log_filename;
    gint                sql_log_slow_ms;
    gint                sql_log_max_size;
    gint                sql_log_cur_size;
    gint                sql_log_file_num;
    gint                sql_log_buffer_size;
    GQueue              *log_filenames_list;
    log_queue           *lq;
} sql_log_t;


void *log_manager(void *user_data);
sql_log_t   *sql_log_t_new();
void sql_log_t_free(sql_log_t *sql_log);
void sql_log_rotate(sql_log_t *sql_log);
void load_sql_filenames(sql_log_t *sql_log, chassis *chas);
void log_sql_slow(sql_log_t *sql_log, network_mysqld_con *con, guint64 query_time);
void log_sql_connect(sql_log_t *sql_log, network_mysqld_con *con);
void log_sql_backend(sql_log_t *sql_log, network_mysqld_con *con, injection *inj);
void log_sql_client(sql_log_t *sql_log, network_mysqld_con *con);
gint sql_log_t_load_options(chassis *srv);

gint assign_sql_log(const char *newval, void *ex_param);
gint assign_sql_log_mode(const char *newval, void *ex_param);
gint assign_sql_log_slow_ms(const char *newval, void *ex_param);
gint assign_sql_log_file_size(const char *newval, void *ex_param);
gint assign_sql_log_file_num(const char *newval, void *ex_param);
gchar *show_sql_log(void *external_param);
gchar *show_sql_log_mode(void *external_param);
gchar *show_sql_log_slow_ms(void *external_param);
gchar *show_sql_log_file_size(void *external_param);
gchar *show_sql_log_file_num(void *external_param);
gint assign_sql_log_buffer_size(const char *newval, void *ex_param);
gchar *show_sql_log_buffer_size(void *ex_param);


#endif /* proxy-sql-log.h */
