#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "chassis-options-utils.h"
#include "network-injection.h"
#include "network-mysqld-packet.h"

#include "proxy-plugin.h"
#include "proxy-sql-log.h"

#define SQL_LOG_BUFFER_DEF_SIZE 10000

const gchar *log_status[] = {"OFF", "ON", "REALTIME"};

void *
log_manager(void *user_data) {
    plugin_thread_param *plugin_params = (plugin_thread_param *) user_data;
    GCond               *g_cond = plugin_params->plugin_thread_cond;
    GMutex              *g_mutex = plugin_params->plugin_thread_mutex;
    sql_log_t           *sql_log = (sql_log_t *)plugin_params->magic_value;
    gint64              end_time;

    g_log_dbproxy(g_message, "%s thread start", PROXY_SQL_LOG_THREAD);

    while (!chassis_is_shutdown()) {
        GString *message = NULL;

        /* OFF */
        if (sql_log->sql_log_type == OFF) {
            if (sql_log->sql_log_fp != NULL) {
                fflush(sql_log->sql_log_fp);
                fclose(sql_log->sql_log_fp);
                sql_log->sql_log_fp = NULL;
            }

            g_mutex_lock(g_mutex);
            while (sql_log->sql_log_type == OFF) {
                end_time =  g_get_monotonic_time() + CHECK_LOG_MANAGER_TIMEOUT * G_TIME_SPAN_SECOND;
                if (!g_cond_wait_until(g_cond, g_mutex, end_time)) {
                    g_log_dbproxy(g_message, "log manager waiting meet timeout");
                } else if (chassis_is_shutdown()) {
                    g_mutex_unlock(g_mutex);
                    g_log_dbproxy(g_message, "log manager thread get exit signal");
                    goto exit;
                }
            }
            g_mutex_unlock(g_mutex);

            /* OFF --> ON open the file first*/
            if (sql_log->sql_log_type != OFF && sql_log->sql_log_fp == NULL) {
                struct stat st;
                sql_log->sql_log_fp = fopen(sql_log->sql_log_filename, "a");
                if (sql_log->sql_log_fp == NULL) {
                    sql_log->sql_log_type = OFF;
                    g_log_dbproxy(g_warning, "open sql log failed");
                    continue;
                }
                fstat(fileno(sql_log->sql_log_fp), &st);
                sql_log->sql_log_cur_size = st.st_size;
            }
        }

        if (chassis_is_shutdown()) break;

        gint q_len = g_queue_get_length(sql_log->lq->log_q);
        if (q_len > sql_log->lq->log_q_max_length) {
            g_atomic_int_set(&sql_log->lq->length_status, 1);
        } else {
            g_atomic_int_set(&sql_log->lq->length_status, 0);
        }

        message = log_queue_pop(sql_log->lq);
        if (message == NULL) {
            // should find a more clever method later
            usleep(LOG_MANAGER_BUFFER_TIMEOUT);
        } else {
            /* write use system call */
            gint fd = fileno(sql_log->sql_log_fp);
            ssize_t write_data_len = pwrite(fd, message->str,
                                            message->len,
                                            (off_t)sql_log->sql_log_cur_size);
            if (write_data_len != message->len) {
                g_log_dbproxy(g_warning, "write sql log file failed");
            }
            if (sql_log->sql_log_type == REALTIME) fsync(fd);
            sql_log->sql_log_cur_size += message->len;
            g_string_free(message, TRUE);
            sql_log_rotate(config->sql_log_mgr);
        }
    }

exit:
    g_log_dbproxy(g_message, "log manager thread will exit");

    g_thread_exit(0);
}

gint
sql_log_t_load_options(chassis *srv)
{
    sql_log_t *sql_log = config->sql_log_mgr;

    g_assert(srv != NULL && sql_log != NULL);

    /* log file name */
    sql_log->sql_log_filename = g_strdup_printf("%s/%s/sql_%s.log",
                               srv->log_path, SQL_LOG_DIR, srv->instance_name);

    /* log type : ON, OFF, REALTIME */
    if (config->sql_log_type) {
        if (strcasecmp(config->sql_log_type, "ON") == 0) {
            sql_log->sql_log_type = ON;
        } else if (strcasecmp(config->sql_log_type, "REALTIME") == 0) {
            sql_log->sql_log_type = REALTIME;
        } else if (strcasecmp(config->sql_log_type, "OFF") == 0) {
            sql_log->sql_log_type = OFF;
        }
    }

    /* log mode : ALL, CLIENT, BACKEND */
    if (config->sql_log_mode) {
        char *token = NULL;
        while ((token = strsep(&config->sql_log_mode, ",")) != NULL) {
            if (strcasecmp(token, "ALL") == 0) {
                sql_log->sql_log_mode = SQL_LOG_ALL;
                break;
            } else if (strcasecmp(token, "CLIENT") == 0) {
                sql_log->sql_log_mode |= SQL_LOG_CLIENT;
            } else if (strcasecmp(token, "BACKEND") == 0) {
                sql_log->sql_log_mode |= SQL_LOG_BACKEND;
            }
        }
    }

    /* gint options */
    if (sql_log->sql_log_max_size < 0) {
        g_log_dbproxy(g_critical, "--sql-log-file-size has to be >= 0, is %d", sql_log->sql_log_max_size);
        return 1;
    }

    if (sql_log->sql_log_file_num < 0) {
        g_log_dbproxy(g_critical, "--sql-log-file-num has to be >= 0, is %d", sql_log->sql_log_file_num);
        return 1;
    }
    load_sql_filenames(sql_log, srv);

    if (sql_log->sql_log_buffer_size < 0) {
        g_log_dbproxy(g_critical, "--sql-log-buffer-size has to be >= 0, is %d", sql_log->sql_log_buffer_size);
        return 1;
    }
    if (sql_log->sql_log_buffer_size == 0) {
        sql_log->lq->log_q_max_length = sql_log->sql_log_buffer_size = SQL_LOG_BUFFER_DEF_SIZE;
    }

    if (sql_log->sql_log_type != OFF) {
        struct stat st;
        sql_log->sql_log_fp = fopen(sql_log->sql_log_filename, "a");
        if (sql_log->sql_log_fp == NULL) {
            g_log_dbproxy(g_critical, "Failed to open sql log file %s", sql_log->sql_log_filename);
            return 1;
        }

        fstat(fileno(sql_log->sql_log_fp), &st);
        sql_log->sql_log_cur_size = st.st_size;
    }

    return 0;
}

sql_log_t *
sql_log_t_new()
{
    sql_log_t *sql_log = g_new0(sql_log_t, 1);

    sql_log->sql_log_fp = NULL;
    sql_log->sql_log_filename = NULL;
    sql_log->sql_log_mode = SQL_LOG_BACKEND;
    sql_log->sql_log_type = ON;
    sql_log->sql_log_cur_size = 0;
    sql_log->lq = log_queue_new();
    sql_log->lq->log_q_max_length = SQL_LOG_BUFFER_DEF_SIZE;
    sql_log->log_filenames_list = g_queue_new();

    return sql_log;
}

void
sql_log_t_free(sql_log_t *sql_log)
{
    if (sql_log == NULL) return;

    if (sql_log->sql_log_fp) fclose(sql_log->sql_log_fp);
    if (sql_log->sql_log_filename) g_free(sql_log->sql_log_filename);
    if (sql_log->log_filenames_list) g_queue_free_full(sql_log->log_filenames_list, g_free);
    if (sql_log->lq) log_queue_free(sql_log->lq);

    g_free(sql_log);

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
                g_log_dbproxy(g_message, "rm log file %s success %d %d %d", rm_logfile, i, max_file_num,
                            g_queue_get_length(*log_filenames_list));
            }
            g_free(rm_logfile);
            i--;
        }
    }

    return ;
}

void
sql_log_rotate(sql_log_t *sql_log)
{
    gchar *rotatename = NULL;

    g_assert(sql_log->sql_log_filename != NULL);

    if (sql_log->sql_log_max_size == 0) {
        return;
    }

    if (sql_log->sql_log_cur_size < sql_log->sql_log_max_size) {
        return ;
    }

    rotatename = get_rotate_file_name(sql_log->sql_log_filename);
    if (sql_log->sql_log_fp != NULL) fclose(sql_log->sql_log_fp);
    if (rename(sql_log->sql_log_filename, rotatename) != 0) {
        g_log_dbproxy(g_critical, "rename log file name to %s failed", rotatename);
    } else {
        register_logfilenames(rotatename, sql_log->sql_log_file_num,
                                            &sql_log->log_filenames_list);
    }
    g_free(rotatename);

    sql_log->sql_log_fp = fopen(sql_log->sql_log_filename, "a");
    sql_log->sql_log_cur_size = 0;
}

gint
assign_sql_log(const char *newval, void *ex_param)
{
    sql_log_t *sql_log = config->sql_log_mgr;
    SQL_LOG_TYPE new_value, old_value;
    gint i = OFF;
    plugin_thread_t *log_manager_thread = NULL;

    g_assert(newval != NULL);

    for (i = OFF; i < REALTIME + 1; i++)
    {
        if (strcasecmp(newval, log_status[i]) == 0)
        {
            new_value = i;
            break;
        }
    }

    if (i > REALTIME)
        return 1;

    old_value = sql_log->sql_log_type;
    sql_log->sql_log_type = new_value;

    /* wake up log manager thread */
    if (old_value == OFF && new_value != old_value) {
        log_manager_thread = g_hash_table_lookup(config->plugin_threads,
                                                        PROXY_SQL_LOG_THREAD);
        g_mutex_lock(&log_manager_thread->thr_mutex);
        g_cond_signal(&log_manager_thread->thr_cond);
        g_mutex_unlock(&log_manager_thread->thr_mutex);
        g_log_dbproxy(g_message, "wake up %s thread", PROXY_SQL_LOG_THREAD);
    }

    return 0;
}

gchar *
show_sql_log(void *ex_param)
{
    sql_log_t *sql_log = config->sql_log_mgr;

    return g_strdup(log_status[sql_log->sql_log_type]);
}

gint
assign_sql_log_mode(const char *newval, void *ex_param)
{
    gint        ret = 0, sql_log_mode = 0;
    gchar       *token = NULL, *sql_log_opt = NULL;
    sql_log_t   *sql_log = config->sql_log_mgr;

    g_assert(newval != NULL);

    sql_log_opt = g_strdup(newval);

    while ((token = strsep(&sql_log_opt, ",")) != NULL) {
        if (strcasecmp(token, "ALL") == 0) {
            sql_log_mode = SQL_LOG_ALL;
            break;
        } else if (strcasecmp(token, "CLIENT") == 0) {
            sql_log_mode |= SQL_LOG_CLIENT;
        } else if (strcasecmp(token, "BACKEND") == 0) {
            sql_log_mode |= SQL_LOG_BACKEND;
        } else {
            ret = 1;
        }
     }
     g_free(sql_log_opt);

    if (ret != 1) {
        sql_log->sql_log_mode = sql_log_mode;
    }

    return ret;
}

gchar *
show_sql_log_mode(void *ex_param)
{
    sql_log_t   *sql_log = config->sql_log_mgr;
    gint        mode_type = sql_log->sql_log_mode;
    gchar       *res = NULL;

    switch (mode_type) {
        case SQL_LOG_ALL:
            res = "ALL";
            break;
        case SQL_LOG_CLIENT:
            res = "CLIENT";
            break;
        case SQL_LOG_BACKEND:
            res = "BACKEND";
            break;
        default:
            res = "Invalid type";
    }

    return g_strdup(res);
}

gint
assign_sql_log_slow_ms(const char *newval, void *ex_param)
{
    sql_log_t   *sql_log = config->sql_log_mgr;

    return set_raw_int_value(newval, &sql_log->sql_log_slow_ms, 0, G_MAXINT32);
}

gchar *
show_sql_log_slow_ms(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    sql_log_t   *sql_log = config->sql_log_mgr;

    return g_strdup_printf("%ld%s", sql_log->sql_log_slow_ms,
                            (opt_param->opt_type == SAVE_OPTS ? "" : "(ms)"));
}

gint
assign_sql_log_file_size(const char *newval, void *ex_param)
{
    sql_log_t   *sql_log = config->sql_log_mgr;

    return set_raw_int_value(newval, &sql_log->sql_log_max_size, 0, G_MAXINT32);
}

gchar *
show_sql_log_file_size(void *ex_param)
{
    sql_log_t   *sql_log = config->sql_log_mgr;
    return g_strdup_printf("%ld", sql_log->sql_log_max_size);
}

gint
assign_sql_log_file_num(const char *newval, void *ex_param)
{
    external_param  *opt_param = (external_param *)ex_param;
    chassis         *srv = opt_param->chas;
    sql_log_t       *sql_log = config->sql_log_mgr;
    gint ret = 1;

    g_assert(newval != NULL);

    if (sql_log->sql_log_file_num == 0) {
        load_sql_filenames(sql_log, srv);
    }
    /* FIXME: need protect */
    return set_raw_int_value(newval, &sql_log->sql_log_file_num, 0, G_MAXINT32);
}

gchar *
show_sql_log_file_num(void *ex_param)
{
    sql_log_t       *sql_log = config->sql_log_mgr;
    return g_strdup_printf("%d", sql_log->sql_log_file_num);
}

gint
assign_sql_log_buffer_size(const char *newval, void *ex_param)
{
    sql_log_t       *sql_log = config->sql_log_mgr;
    gint ret = 1;

    g_assert(newval != NULL);

    ret = set_raw_int_value(newval, &sql_log->sql_log_buffer_size, 0, G_MAXINT32);
    if (ret == 0) {
        sql_log->lq->log_q_max_length = sql_log->sql_log_buffer_size;
    }

    return ret;
}

gchar *
show_sql_log_buffer_size(void *ex_param)
{
    sql_log_t       *sql_log = config->sql_log_mgr;
    return g_strdup_printf("%d", sql_log->sql_log_buffer_size);
}

void
load_sql_filenames(sql_log_t *sql_log, chassis *chas) {
    GDir    *rldir = NULL;
    const gchar *rlde = NULL;
    gchar   *filename_prefix = NULL;
    gchar   *dirname = NULL;

    g_assert(chas != NULL);

    filename_prefix = g_strdup_printf("sql_%s.log_", chas->instance_name);
    dirname = g_strdup_printf("%s/%s", chas->log_path, SQL_LOG_DIR);
    g_log_dbproxy(g_message, "load_files_from_dir %s", dirname);

    if ((rldir = g_dir_open(dirname, 0, NULL)) == NULL) {
        g_log_dbproxy(g_critical, "open dir %s failed", dirname);
        goto funcexit;
    }

    if (sql_log->log_filenames_list == NULL) {
        sql_log->log_filenames_list = g_queue_new();
    }
    while (rlde = g_dir_read_name(rldir)) {
        /* exclude current write file */
        if (strncmp(rlde, filename_prefix, strlen(filename_prefix)) == 0) {
            gchar *abslogfilename = g_strdup_printf("%s/%s", dirname, rlde);
            g_queue_insert_sorted(sql_log->log_filenames_list, abslogfilename,
                                            (GCompareDataFunc)strcmp, NULL);
            g_log_dbproxy(g_message, "load file %s success", abslogfilename);
        }
    }

    g_dir_close(rldir);
    g_log_dbproxy(g_message, "load_files_from_dir %s success", dirname);

funcexit:
    g_free(filename_prefix);
    g_free(dirname);

    return ;
}

void
log_sql_client(sql_log_t *sql_log, network_mysqld_con *con)
{
    GString *message = NULL;
    guint64 com_type = 0;

    g_assert(sql_log != NULL);

    if (sql_log->sql_log_type == OFF ||
        !(sql_log->sql_log_mode & SQL_LOG_CLIENT)) {
        return;
    }

    message = g_string_sized_new(sizeof("2004-01-01T00:00:00.000Z"));
    /* get current time */
    chassis_log_update_timestamp(message, CHASSIS_RESOLUTION_US);
    com_type = con->conn_status_var.cur_query_com_type;
    /* build message string */
    g_string_append_printf(message, ": C:%s C_db:%s C_usr:%s %s %s\n",
                                            NETWORK_SOCKET_SRC_NAME(con->client),
                                            NETWORK_SOCKET_DB_NAME(con->client),
                                            NETWORK_SOCKET_USR_NAME(con->client),
                                            GET_COM_NAME(com_type),
                                            strlen(con->conn_status_var.cur_query) > 0 ?
                                               con->conn_status_var.cur_query : "");

    if (log_queue_push(sql_log->lq, message) < 0) {
        g_string_free(message, TRUE);
    }
    return ;
}

void
log_sql_backend(sql_log_t *sql_log, network_mysqld_con *con, injection *inj)
{
    gfloat latency_ms = 0.0;
    GString *message = NULL;
    GString *begin_time = NULL;

    if (sql_log->sql_log_type == OFF ||
        !(sql_log->sql_log_mode & SQL_LOG_BACKEND) ||
        (sql_log->sql_log_mode != SQL_LOG_ALL && IS_IMPLICIT_INJ(inj))) {
        return;
    }

    latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
    if ((gint)latency_ms < sql_log->sql_log_slow_ms) return;

    message = g_string_sized_new(sizeof("2004-01-01T00:00:00.000Z"));

    begin_time = chassis_log_microsecond_tostring(con->conn_status_var.cur_query_start_time,
                                            CHASSIS_RESOLUTION_US);
    /* get current time */
    chassis_log_update_timestamp(message, CHASSIS_RESOLUTION_US);
    g_string_append_printf(message, ": C_begin:%s C:%s C_db:%s C_usr:%s S:%s(thread_id:%u) S_db:%s "
                                    "S_usr:%s inj(type:%d bytes:%lu rows:%lu) %.3f(ms) %s %s:%s\n",
                                begin_time->str,
                                NETWORK_SOCKET_SRC_NAME(con->client),
                                NETWORK_SOCKET_DB_NAME(con->client),
                                NETWORK_SOCKET_USR_NAME(con->client),
                                NETWORK_SOCKET_DST_NAME(con->server),
                                NETWORK_SOCKET_THREADID(con->server),
                                NETWORK_SOCKET_DB_NAME(con->server),
                                NETWORK_SOCKET_USR_NAME(con->server),
                                inj->id, inj->bytes, inj->rows,
                                latency_ms, inj->qstat.query_status == MYSQLD_PACKET_OK ? "OK" : "ERR",
                                GET_COM_STRING(inj->query));
    g_string_free(begin_time, TRUE);


    if (log_queue_push(sql_log->lq, message) < 0) {
        g_string_free(message, TRUE);
    }

    return ;
}

void
log_sql_slow(sql_log_t *sql_log, network_mysqld_con *con, guint64 query_time)
{
    guint64 com_type = 0;
    GString *message = NULL;

    if (sql_log->sql_log_type == OFF) {
        return;
    }

    com_type = con->conn_status_var.cur_query_com_type;
    message = g_string_sized_new(sizeof("2004-01-01T00:00:00.000Z"));
    /* get current time */
    chassis_log_update_timestamp(message, CHASSIS_RESOLUTION_US);
    g_string_append_printf(message, ": C:%s C_db:%s C_usr:%s [Slow Query] %.3f(ms) %s %s\n",
                    NETWORK_SOCKET_SRC_NAME(con->client),
                    NETWORK_SOCKET_DB_NAME(con->client),
                    NETWORK_SOCKET_USR_NAME(con->client),
                    (gfloat)query_time/1000.0,
                    GET_COM_NAME(com_type),
                    strlen(con->conn_status_var.cur_query) > 0 ?
                            con->conn_status_var.cur_query : "");

    if (log_queue_push(sql_log->lq, message) < 0) {
        g_string_free(message, TRUE);
    }

    return;
}

void
log_sql_connect(sql_log_t *sql_log, network_mysqld_con *con)
{
    GString *message = NULL;

    if (sql_log->sql_log_type == OFF ||
        !(sql_log->sql_log_mode & SQL_LOG_CLIENT)) {
        return;
    }

    message = g_string_sized_new(sizeof("2004-01-01T00:00:00.000Z"));
    /* get current time */
    chassis_log_update_timestamp(message, CHASSIS_RESOLUTION_US);

    g_string_append_printf(message, ": C:%s C_db:%s C_usr:%s Connect %s@%s at event_thread(%d)\n",
                        NETWORK_SOCKET_SRC_NAME(con->client),
                        NETWORK_SOCKET_DB_NAME(con->client),
                        NETWORK_SOCKET_USR_NAME(con->client),
                        NETWORK_SOCKET_USR_NAME(con->client),
                        NETWORK_SOCKET_SRC_NAME(con->client),
                        chassis_event_get_threadid());

    if (log_queue_push(sql_log->lq, message) < 0) {
        g_string_free(message, TRUE);
    }
    return ;
}

