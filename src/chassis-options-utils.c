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

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "chassis-mainloop.h"
#include "chassis-options-utils.h"
#include "chassis-filter.h"
#include "chassis-frontend.h"
#include "chassis-log.h"
#include "chassis-options.h"
#include "chassis-plugin.h"
#include "chassis-path.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x)-1
#define C_S(x) x, strlen(x)
#define S(x) x->str, x->len

/* main entries */
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
try_get_int_value(const gchar *option_value, gint *return_value)
{
    gchar   *endptr = NULL;
    gint    value = 0;

    g_assert(option_value != NULL);

    value = strtol(option_value, &endptr, 0);
    if ((endptr != NULL && *endptr != '\0') ||
            (errno == ERANGE && (value == G_MAXINT32 || value == G_MININT32)) ||
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
    //gdouble adjust_value = 0.0;

    g_assert(option_value != NULL);

    value = strtod(option_value, &endptr);
    if ((endptr != NULL && *endptr != '\0') ||
            (errno == ERANGE && (value == G_MAXFLOAT || value == G_MINFLOAT)) ||
            endptr == option_value)
        return FALSE;

    *return_value = value;
    return TRUE;
}

static gint
set_atomic_int64_option(volatile gint *ptr_int_value, const gchar *option_value,
                        gint64 min_limit, gint64 max_limit)
{
    int ret = 1;
    gint64 value = 0;

    g_assert(ptr_int_value != NULL && option_value != NULL);

    if (try_get_int64_value(option_value, &value))
    {
        if (value >= min_limit && value < max_limit)
        {
            g_atomic_int_set(ptr_int_value, value);
            ret = 0;
        }
    }

    return ret;
}

static gint
set_rwlock_int64_option(gint *ptr_int_value, const gchar *option_value,
                        GRWLock *log_lock, gint64 min_limit, gint64 max_limit)
{
    gint ret = 1;
    gint64 value = 0;

    g_assert(ptr_int_value != NULL && option_value != NULL);

    if (try_get_int64_value(option_value, &value))
    {
        if (value >= min_limit && value < max_limit)
        {
            g_rw_lock_writer_lock(log_lock);
            *ptr_int_value = value;
            g_rw_lock_writer_unlock(log_lock);
            ret = 0;
        }
    }

    return ret;
}

static gint
set_raw_int64_value(const gchar *newval, gint64 *param)
{
    gint64 raw_value = 0;
    gint ret = 1;

    g_assert(newval != NULL);

    if (try_get_int64_value(newval, &raw_value)) {
        *(param) = raw_value;
        ret = 0;
    }

    return ret;
}

gint
set_raw_int_value(const gchar *newval, gint *param, gint min, gint max)
{
    gint raw_value = 0;
    gint ret = 1;

    g_assert(newval != NULL);

    if (try_get_int_value(newval, &raw_value)) {
        if (raw_value >= min &&  raw_value < max) {
            *(param) = raw_value;
            ret = 0;
        }
    }

    return ret;
}

static gint
set_rwlock_double_option(gdouble *ptr_double_value, const gchar *option_value,
                        GRWLock *log_lock, gdouble min_limit, gdouble max_limit)
{
    int ret = 1;
    gdouble value = 0;

    g_assert(ptr_double_value != NULL && option_value != NULL);

    if (try_get_double_value(option_value, &value))
    {
        if (value >= min_limit && value < max_limit)
        {
            g_rw_lock_writer_lock(log_lock);
            *ptr_double_value = value;
            g_rw_lock_writer_unlock(log_lock);
            ret = 0;
        }
    }

    return ret;
}

static int
reset_thread_running(chassis *chas)
{
    network_backends_t* bs = chas->backends;
    int i = 0;

    g_rw_lock_reader_lock(&bs->backends_lock);

    guint count = network_backends_count(bs);
    for (i = 0; i < count; ++i)
    {
        network_backend_t* backend = network_backends_get(bs, i);
        if (backend == NULL) continue;
        g_atomic_int_set(&backend->thread_running, 0);
    }

    g_rw_lock_reader_unlock(&bs->backends_lock);

    g_log_dbproxy(g_message, "[filter][reset thread running][success]");

    return 0;
}

gint
assign_verbose_shutdown(const char *newval, void *ex_param)
{
    return 0;
}

gchar *
show_verbose_shutdown(void *ex_param)
{
    return g_strdup("");
}

gint
assign_basedir(const char *newval, void *ex_param)
{
    return 0;
}
gchar *
show_basedir(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return g_strdup(srv->base_dir);
}

gint
assign_plugin_dir(const char *newval, void *ex_param)
{
    return 0;
}
gchar *
show_plugin_dir(void *ex_param)
{
    return g_strdup("");
}

gchar *
show_plugins(void *ex_param)
{
    return g_strdup("proxy, admin");
}

gint
assign_log_level(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gint i = 0;

    g_assert(newval != NULL);

    for (i = 0; log_lvl_map[i].name; i++)
    {
        if (strcmp(log_lvl_map[i].name, newval) == 0)
        {
            g_mutex_lock(&(srv->log->log_mutex));
            srv->log->min_lvl = log_lvl_map[i].lvl;
            g_mutex_unlock(&(srv->log->log_mutex));
            break;
        }
    }

    if (i >= LOG_LVL_MAP_SIZE)
        return 1;

    return 0;
}
gchar *
show_log_level(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gint i = 0;

    for (i = 0; log_lvl_map[i].name; i++)
    {
        if (srv->log->min_lvl == log_lvl_map[i].lvl)
        {
            return g_strdup(log_lvl_map[i].name);
        }
    }
    return g_strdup("");
}

gint
assign_log_path(const char *newval, void *ex_param)
{
    return 0;
}
gchar *
show_log_path(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return g_strdup(srv->log_path);
}

gint
assign_log_bt_on_crash(const char *newval, void *ex_param)
{
    return 0;
}
gchar *
show_log_bt_on_crash(void *ex_param)
{
    return g_strdup("ON");
}

gchar *
show_keepalive(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return (srv->auto_restart != 0 ? g_strdup("TRUE") : g_strdup("FALSE"));
}

gchar *
show_max_open_files(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return g_strdup_printf("%ld", srv->max_files_number);
}

gint
assign_max_connections(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return set_raw_int_value(newval, &srv->proxy_max_connections, 0, G_MAXINT32);
}

gchar *
show_max_connections(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->proxy_max_connections);
}

gint
assign_long_wait_time(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return set_raw_int_value(newval, &srv->long_wait_time, 0, G_MAXINT32);
}
gchar *
show_long_wait_time(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return g_strdup_printf("%d%s", srv->long_wait_time, (opt_param->opt_type == SAVE_OPTS ? "" : "(ms)"));
}

gint
assign_long_query_time(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return set_raw_int_value(newval, &srv->long_query_time, 0, G_MAXINT32);
}
gchar *
show_long_query_time(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->long_query_time, (opt_param->opt_type == SAVE_OPTS ? "" : "(ms)"));
}

gint
assign_time_range_base(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return set_raw_int_value(newval, &srv->query_response_time_range_base, 2, G_MAXINT32);
}
gchar *
show_time_range_base(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    return g_strdup_printf("%d", srv->query_response_time_range_base);
}

gint
assign_time_stats(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return set_raw_int_value(newval, &srv->query_response_time_stats, 0, G_MAXINT32);
}
gchar *
show_time_stats(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->query_response_time_stats);
}

gchar *
show_event_threads(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%u", srv->event_thread_count);
}

gchar *
show_lua_path(void *ex_param)
{
    return g_strdup("");
}

gchar *
show_user(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup(srv->user);
}

gchar *
show_instance(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup(srv->instance_name);
}

gint
assign_wait_timeout(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);

    return set_atomic_int64_option(&srv->wait_timeout,
                                   newval, 0, G_MAXINT64);
}
gchar *
show_wait_timeout(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->wait_timeout, (opt_param->opt_type == SAVE_OPTS ? "" : "(s)"));
}

gint
assign_shutdown_timeout(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);
    return set_atomic_int64_option(&srv->shutdown_timeout,
                                  newval, 1, G_MAXINT64);
}
gchar *
show_shutdown_timeout(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->shutdown_timeout, (opt_param->opt_type == SAVE_OPTS ? "" : "(s)"));
}

gint
assign_db_connetion_idle_timeout(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return set_raw_int_value(newval, &srv->db_connection_idle_timeout, 0, G_MAXINT32);

}
gchar *
show_db_connection_idle_timeout(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->db_connection_idle_timeout, (opt_param->opt_type == SAVE_OPTS ? "" : "(s)"));
}

gint
assign_db_connection_max_age(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return set_raw_int_value(newval, &srv->db_connection_max_age, 0, G_MAXINT32);
}
gchar *
show_db_connection_max_age(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->db_connection_max_age, (opt_param->opt_type == SAVE_OPTS ? "" : "(s)"));
}

gint
assign_mysql_version(const char *newval, void *ex_param)
{
    gint ret = 0;
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);

    if (strcmp(newval, "5.5") == 0) {
        srv->my_version = MYSQL_55;
    } else if (strcmp(newval, "5.6") == 0) {
        srv->my_version = MYSQL_56;
    } else if (strcmp(newval, "5.7") == 0) {
        srv->my_version = MYSQL_57;
    } else {
        ret = 1;
    }
    return ret;
}
gchar *
show_mysql_version(void *ex_param)
{
    gchar *mysql_version = NULL;
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    if (srv->my_version == MYSQL_55) {
        mysql_version = "5.5";
    } else if (srv->my_version == MYSQL_56) {
        mysql_version = "5.6";
    } else {
        mysql_version = "5.7";
    }
    return g_strdup(mysql_version);
}

gint
assign_lastest_query_num(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gint ret = 1;

    g_assert(newval != NULL);

    ret = set_atomic_int64_option(&srv->proxy_reserved->lastest_query_num,
                                    newval, 0, G_MAXINT64);
    if (g_atomic_int_get(&srv->proxy_reserved->lastest_query_num) == 0)
    {
        sql_reserved_query_rebuild(srv->proxy_reserved, 0);
    }
    return ret;
}
gchar *
show_lastest_query_num(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->proxy_reserved->lastest_query_num);
}

/*
 *  freq = AccessTimes/time_windows
 *
 *  freq is fixed
 *   if (AccessTimes > ratio) time_windows = AccessTimes/freq
 *   else time_windows = ratio/freq
 *
 *   if (query's lastest_time - query's first_time < time_windows) bypass
 *   else calulate whether query should be added to filter.
 */
gint
assign_time_threshold(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);
    return set_atomic_int64_option(&srv->proxy_reserved->query_filter_time_threshold,
                                    newval, -1, G_MAXINT64);
}
gchar *
show_time_threshold(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->proxy_reserved->query_filter_time_threshold);
}

gint
assign_freq_threshold(const char *newval, void *ex_param)
{
    gint ret = 1;
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    sql_reserved_query *rq  = srv->proxy_reserved;
    g_assert(newval != NULL);
    ret = set_rwlock_double_option(&rq->query_filter_frequent_threshold,
                                   newval,
                                   &rq->rq_lock, 1e-3, G_MAXFLOAT);
    if (ret == 0)
    {
        gint gap_threadhold = 0;
        gfloat freq = 0.0;

        gap_threadhold = g_atomic_int_get(&rq->access_num_per_time_window);
        g_rw_lock_reader_lock(&rq->rq_lock);
        freq = rq->query_filter_frequent_threshold;
        g_rw_lock_reader_unlock(&rq->rq_lock);

        if (freq > 1e-3 && gap_threadhold > 0)
        {
            set_freq_time_windows(srv->proxy_reserved, freq, gap_threadhold);
        }
    }

    return ret;
}
gchar *
show_freq_threshold(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    sql_reserved_query *rq = srv->proxy_reserved;

    return g_strdup_printf("%f", rq->query_filter_frequent_threshold);
}

gint
assign_access_ratio(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    sql_reserved_query *rq  = srv->proxy_reserved;
    gfloat freq = 0.0;
    gint gap_threadhold = 0;
    gint ret = 1;

    g_assert(newval != NULL);
    ret = set_atomic_int64_option(&rq->access_num_per_time_window,
                            newval, 1, G_MAXINT64);
    if (ret == 0) {
        g_rw_lock_reader_lock(&rq->rq_lock);
        gap_threadhold = g_atomic_int_get(&rq->access_num_per_time_window);
        freq = rq->query_filter_frequent_threshold;
        g_rw_lock_reader_unlock(&rq->rq_lock);

        if (freq > 1e-3 && gap_threadhold > 0) {
            set_freq_time_windows(srv->proxy_reserved, freq, gap_threadhold);
        }
    }

    return ret;
}

gchar *
show_access_ratio(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->proxy_reserved->access_num_per_time_window);
}

gint
assign_backend_max_thread_running(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);
    return set_raw_int_value(newval, &srv->max_backend_tr, 0, G_MAXINT32);
}
gchar *
show_backend_max_thread_running(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->max_backend_tr);
}

gint
assign_thread_running_sleep_delay(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);

    return set_raw_int_value(newval, &srv->thread_running_sleep_delay, 0, G_MAXINT32);
}
gchar *
show_thread_running_sleep_delay(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d%s", srv->thread_running_sleep_delay, (opt_param->opt_type == SAVE_OPTS ? "" : "(ms)"));
}

static gint
set_filter_flag(const char *newval, const char *flag_name, chassis *chas)
{
    gint    flag = -1;
    gint    ret = 1;
    gsize   keysize = 0;

    g_assert(newval != NULL);

    if (strcasecmp(newval, "ON") == 0) {
        flag = 1;
    } else if (strcasecmp(newval, "OFF") == 0) {
        flag = 0;
    }

    if (flag > -1) {
        g_rw_lock_writer_lock(&chas->proxy_filter->sql_filter_lock);
        if (strleq(flag_name, keysize, C("auto-filter-flag"))) {
            chas->proxy_filter->auto_filter_flag = flag;
        } else {
            chas->proxy_filter->manual_filter_flag = flag;
        }
        g_rw_lock_writer_unlock(&chas->proxy_filter->sql_filter_lock);
        ret = 0;
    }

    return ret;
}

static gchar *
get_filter_flag_str(gint filter_flag)
{
    return (filter_flag == 0) ? "OFF" : "ON";
}

gint
assign_auto_filter_flag(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;

    return set_filter_flag(newval, "auto-filter-flag", opt_param->chas);
}
gchar *
show_auto_filter_flag(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup(get_filter_flag_str(srv->proxy_filter->auto_filter_flag));
}

gint
assign_manual_filter_flag(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;

    return set_filter_flag(newval, "manual-filter-flag", opt_param->chas);
}
gchar *
show_manual_filter_flag(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup(get_filter_flag_str(srv->proxy_filter->manual_filter_flag));
}

gint
assign_remove_backend_timeout(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    g_assert(newval != NULL);
    return set_atomic_int64_option(&srv->backends->remove_backend_timeout,
                                   newval, 0, G_MAXUINT32);
}

gchar *
show_remove_backend_timeout(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%u%s", srv->backends->remove_backend_timeout, (opt_param->opt_type == SAVE_OPTS ? "" : "(s)"));
}

gint
assign_blacklist_file(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gint ret = 1;

    g_assert(newval != NULL);

    gchar *blacklist_file_dir = g_path_get_dirname(newval);
    if (-1 == faccessat(0, blacklist_file_dir, F_OK | X_OK | W_OK, AT_EACCESS)) {
        g_log_dbproxy(g_warning, "error happened on file %s: %s", newval, g_strerror(errno));
    } else {
        g_rw_lock_writer_lock(&srv->proxy_filter->sql_filter_lock);
        gchar *tmp_str = srv->proxy_filter->blacklist_file;
        srv->proxy_filter->blacklist_file = g_strdup(newval);
        chassis_resolve_path(srv->base_dir, &srv->proxy_filter->blacklist_file);
        g_rw_lock_writer_unlock(&srv->proxy_filter->sql_filter_lock);

        ret = load_sql_filter_from_file(srv->proxy_filter);
        if (ret != 0) {
            /* if load failed, rollback to old name. */
            g_rw_lock_writer_lock(&srv->proxy_filter->sql_filter_lock);
            g_free(srv->proxy_filter->blacklist_file);
            srv->proxy_filter->blacklist_file = tmp_str;
            g_rw_lock_writer_unlock(&srv->proxy_filter->sql_filter_lock);
        } else 
            g_free(tmp_str);
    }

    if (blacklist_file_dir)
        g_free(blacklist_file_dir);

    return ret;
}

gchar *
show_blacklist_file(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gchar *blacklist_filename = NULL;

    g_rw_lock_reader_lock(&srv->proxy_filter->sql_filter_lock);
    if (srv->proxy_filter->blacklist_file != NULL) {
        blacklist_filename = g_strdup(srv->proxy_filter->blacklist_file);
    } else {
        blacklist_filename = g_strdup("");
    }
    g_rw_lock_reader_unlock(&srv->proxy_filter->sql_filter_lock);

    return blacklist_filename;
}

gint
assign_log_trace_modules(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return set_raw_int_value(newval, &srv->log->log_trace_modules, 0, G_MAXINT32);
}

gint
assign_db_connect_timeout(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gboolean ret = TRUE;
    gdouble new_value = 0.0;

    if((ret = try_get_double_value(newval, &new_value))) {
        if(new_value >= 0.0) {
            srv->db_connect_timeout = new_value;
        } else {
            ret = FALSE;
        }
    }
    return (ret? 0: -1);
}

gchar *
show_log_trace_modules(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%d", srv->log->log_trace_modules);
}

gchar *
show_version(void *ex_param)
{
    return g_strdup(CHASSIS_BUILD_TAG);
}

gchar *
show_daemon(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gchar *res = NULL;

    if (srv->daemon_mode) {
        res = "TRUE";
    } else {
        res = "FALSE";
    }

    return g_strdup(res);
}

gchar* show_db_connect_timeout(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;

    return g_strdup_printf("%lf", srv->db_connect_timeout);
}

gint
assign_backend_monitor_pwds(const char *newval, void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gint ret = 1;
    gchar *user = NULL, *pwd = NULL;
    gchar *cur_pwd_info = NULL;
    gchar *tmp_for_free = NULL;

    g_assert(newval != NULL);

    cur_pwd_info = g_strdup(newval);
    tmp_for_free = cur_pwd_info;

    if ((user = strsep(&cur_pwd_info, ":")) != NULL && (pwd = strsep(&cur_pwd_info, ":")) != NULL) {
        ret = network_backends_set_monitor_pwd(srv->backends, user, pwd, FALSE);
    }
    g_free(tmp_for_free);

    return ret;
}

gchar *
show_assign_backend_monitor_pwds(void *ex_param)
{
    external_param *opt_param = (external_param *)ex_param;
    chassis *srv = opt_param->chas;
    gchar *u = NULL;

    g_rw_lock_reader_lock(&srv->backends->user_mgr_lock);
    if (srv->backends->monitor_user) {
        u = g_strdup_printf("%s:%s", srv->backends->monitor_user, srv->backends->monitor_encrypt_pwd);
    } else {
        u = g_strdup("");
    }
    g_rw_lock_reader_unlock(&srv->backends->user_mgr_lock);

    return u;
}
    
static int
chassis_options_save(GKeyFile *keyfile, chassis_options_t *opts, chassis  *chas)
{
    GList *node = NULL;

    g_assert(chas != NULL && opts != NULL);

    for (node = opts->options; node; node = node->next) {
        chassis_option_t *opt = node->data;

        if (opt->show_hook != NULL && CAN_SAVE_OPTS(opt->opt_property)) {
            external_param *t_param = (external_param *)g_new0(external_param, 1);
            gchar          *value = NULL;

            t_param->L = NULL;
            t_param->tables = NULL;
            if (strcmp(opt->long_name, "tables") == 0) {
                t_param->opt_type = SAVE_SHARD_TABLE;
            } else {
                t_param->opt_type = SAVE_OPTS;
            }
            t_param->chas = (void *)chas;

            value = opt->show_hook((void *)t_param);
            if (value != NULL) {
                g_key_file_set_value(keyfile, "mysql-proxy", opt->long_name, value);
            } else {
                g_key_file_set_value(keyfile, "mysql-proxy", opt->long_name, "");
            }

            g_free(value);
            if (t_param != NULL) g_free(t_param);
        }
    }

    return 0;
}

int
save_config(chassis *chas) {
    GKeyFile *keyfile = g_key_file_new();
    network_backends_t *bs = chas->backends;

    g_key_file_set_list_separator(keyfile, ',');
    GError *gerr = NULL;

    if (FALSE == g_key_file_load_from_file(keyfile, bs->default_file, G_KEY_FILE_KEEP_COMMENTS, &gerr)) {
        g_log_dbproxy(g_critical, "g_key_file_load_from_file: %s", gerr->message);
        g_error_free(gerr);
        g_key_file_free(keyfile);
        return 1;
    }

    GString *master = g_string_new(NULL);
    GString *slave  = g_string_new(NULL);
    guint i;
    GPtrArray *backends = bs->backends;

    /* backends */
    g_rw_lock_reader_lock(&bs->backends_lock);
    guint len = backends->len;
    for (i = 0; i < len; ++i) {
        network_backend_t *backend = g_ptr_array_index(backends, i);
        /* if the backend state is BACKEND_STATE_OFFLINING or BACKEND_STATE_OFFLINE or  BACKEND_STATE_REMOVING don't save it to config. */
        if (backend->state == BACKEND_STATE_OFFLINING || backend->state == BACKEND_STATE_OFFLINE
                || backend->state == BACKEND_STATE_REMOVING) {
            continue;
        }
        if (backend->type == BACKEND_TYPE_RW) {
            g_string_append_printf(master, ",%s", backend->addr->name->str);
        } else if (backend->type == BACKEND_TYPE_RO) {
            g_string_append_printf(slave, ",%s", backend->addr->name->str);

            if (backend->slave_tag && backend->slave_tag->len > 0) g_string_append_printf(slave, "$%s", backend->slave_tag->str);
            if (backend->weight != 1) g_string_append_printf(slave, "@%d", backend->weight);
        }
    }
    g_rw_lock_reader_unlock(&bs->backends_lock);

    if (master->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "proxy-backend-addresses", master->str+1);
    } else {
        g_key_file_set_value(keyfile, "mysql-proxy", "proxy-backend-addresses", "");
    }
    if (slave->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "proxy-read-only-backend-addresses", slave->str+1);
    } else {
        g_key_file_set_value(keyfile, "mysql-proxy", "proxy-read-only-backend-addresses", "");
    }

    g_string_free(master, TRUE);
    g_string_free(slave, TRUE);

    /* user */
    GString *pwds = g_string_new(NULL);
    GString *user_hosts = g_string_new(NULL);
    GString *backends_tag = g_string_new(NULL);
    GString *monitor_user = g_string_new(NULL);

    g_rw_lock_reader_lock(&bs->user_mgr_lock);
    for (i = 0; i < bs->raw_pwds->len; ++i) {
        raw_user_info *rwi = g_ptr_array_index(bs->raw_pwds, i);
        if (g_strcmp0(rwi->username, WHITELIST_USER) != 0) {
            g_string_append_c(pwds, ',');
            g_string_append_printf(pwds, "%s:%s", rwi->username, rwi->encrypt_pwd);
        }

        if (rwi->user_hosts != NULL) {
            g_string_append_c(user_hosts, ',');
            g_string_append_printf(user_hosts, "%s@%s", rwi->username, rwi->user_hosts);
        }

        if (rwi->backends != NULL) {
            g_string_append_c(backends_tag, ',');
            g_string_append_printf(backends_tag, "%s@%s", rwi->username, rwi->backends);
        }
    }

    if (bs->monitor_user) {
        g_string_append_printf(monitor_user, "%s:%s", bs->monitor_user, bs->monitor_encrypt_pwd);
    }

    g_rw_lock_reader_unlock(&bs->user_mgr_lock);

    if (pwds->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "pwds", pwds->str+1);
    } else {
        g_key_file_set_value(keyfile, "mysql-proxy", "pwds", "");
    }
    if (user_hosts->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "user-hosts", user_hosts->str+1);
    } else {
        g_key_file_set_value(keyfile, "mysql-proxy", "user-hosts", "");
    }
    if (backends_tag->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "user-backends", backends_tag->str+1);
    } else {
        g_key_file_set_value(keyfile, "mysql-proxy", "user-backends", "");
    }
    if (monitor_user->len != 0) {
        g_key_file_set_value(keyfile, "mysql-proxy", "backend-monitor-pwds", monitor_user->str);
    }

    g_string_free(pwds, TRUE);
    g_string_free(user_hosts, TRUE);
    g_string_free(backends_tag, TRUE);
    g_string_free(monitor_user, TRUE);

    g_rw_lock_reader_lock(&bs->au->admin_user_lock);
    g_key_file_set_value(keyfile, "mysql-proxy", "admin-username", bs->au->name);
    g_key_file_set_value(keyfile, "mysql-proxy", "admin-password", bs->au->password);
    GString *admin_hosts = admin_user_hosts_show(bs->au);
    if (admin_hosts) {
        g_key_file_set_value(keyfile, "mysql-proxy", "admin-user-hosts", admin_hosts->str);
        g_string_free(admin_hosts, TRUE);
    }
    g_rw_lock_reader_unlock(&bs->au->admin_user_lock);

    /* key/value options */
    chassis_options_save(keyfile, chas->opts, chas);
    for (i = 0; i < chas->modules->len; i++)
    {
        chassis_options_t *plugin_opts = NULL;
        chassis_plugin *p = chas->modules->pdata[i];
        if (NULL != (plugin_opts = chassis_plugin_get_options(p))) {
            chassis_options_save(keyfile, plugin_opts, chas);
        }
    }

    /* do save */
    gsize file_size = 0;
    gchar *file_buf = g_key_file_to_data(keyfile, &file_size, NULL);
    if (FALSE == g_file_set_contents(bs->default_file, file_buf, file_size, &gerr)) {
        g_log_dbproxy(g_critical, "g_file_set_contents: %s", gerr->message);
        g_free(file_buf);
        g_error_free(gerr);
        g_key_file_free(keyfile);
        return 2;
    }

    g_log_dbproxy(g_message, "saving config file succeed");
    g_free(file_buf);
    g_key_file_free(keyfile);

    return 0;
}

gboolean
opt_match(const char *str, const char *prefix)
{
    gchar   *percent_pos = NULL;
    gsize   cmp_size = 0;
    gboolean match = FALSE;

    g_assert(str != NULL && prefix != NULL);

    percent_pos = g_strstr_len(prefix, strlen(prefix), "%");
    if (percent_pos != NULL) {
        cmp_size = (gsize)(percent_pos - (const gchar *)prefix);
        if (g_ascii_strncasecmp(str, prefix, cmp_size) == 0) {
            match = TRUE;
        }
    } else {
        if (strlen(str) == strlen(prefix) &&
                    g_ascii_strcasecmp(str, prefix) == 0) {
            match = TRUE;
        }
    }
 
    return match;
}
