#ifndef __CHASSIS_OPTIONS_UTILS_H__
#define __CHASSIS_OPTIONS_UTILS_H__

#include <glib.h>
#include "chassis-exports.h"
#include "chassis-mainloop.h"

#define RM_SHARD_TABLE      1
#define SHOW_SHARD_TABLE    2
#define SAVE_SHARD_TABLE    3
#define ADD_SHARD_TABLE     4
#define SHOW_OPTS           5
#define SAVE_OPTS           6


#define ASSIGN_KEY_VALUE_PROPERTY  0x01
#define SHOW_OPTS_PROPERTY   0x02
#define SAVE_OPTS_PROPERTY   0x04

#define ALL_OPTS_PROPERTY  (ASSIGN_KEY_VALUE_PROPERTY | SHOW_OPTS_PROPERTY | SAVE_OPTS_PROPERTY)
#define ASSIGN_SHOW        (ASSIGN_KEY_VALUE_PROPERTY | SHOW_OPTS_PROPERTY)
#define ASSIGN_SAVE        (ASSIGN_KEY_VALUE_PROPERTY | SAVE_OPTS_PROPERTY)
#define SHOW_SAVE          (SAVE_OPTS_PROPERTY | SHOW_OPTS_PROPERTY)

#define CAN_ASSGIN_OPTS(opt_property) ((opt_property) & ASSIGN_KEY_VALUE_PROPERTY)
#define CAN_SHOW_OPTS(opt_property) ((opt_property) & SHOW_OPTS_PROPERTY)
#define CAN_SAVE_OPTS(opt_property) ((opt_property) & SAVE_OPTS_PROPERTY)

typedef struct external_param {
    void *L;
    chassis *chas;
    const char *tables;
    gint opt_type;
    void        *magic_value;
} external_param;


/* assign utils */
CHASSIS_API gint assign_verbose_shutdown(const char *newval, void *ex_param);
CHASSIS_API gint assign_basedir(const char *newval, void *ex_param);
CHASSIS_API gint assign_plugin_dir(const char *newval, void *ex_param);
CHASSIS_API gint assign_log_level(const char *newval, void *ex_param);
CHASSIS_API gint assign_log_path(const char *newval, void *ex_param);
CHASSIS_API gint assign_log_bt_on_crash(const char *newval, void *ex_param);
CHASSIS_API gint assign_max_connections(const char *newval, void *ex_param);
CHASSIS_API gint assign_long_wait_time(const char *newval, void *ex_param);
CHASSIS_API gint assign_long_query_time(const char *newval, void *ex_param);
CHASSIS_API gint assign_time_range_base(const char *newval, void *ex_param);
CHASSIS_API gint assign_time_stats(const char *newval, void *ex_param);
CHASSIS_API gint assign_wait_timeout(const char *newval, void *ex_param);
CHASSIS_API gint assign_shutdown_timeout(const char *newval, void *ex_param);
CHASSIS_API gint assign_db_connetion_idle_timeout(const char *newval, void *ex_param);
CHASSIS_API gint assign_db_connection_max_age(const char *newval, void *ex_param);
CHASSIS_API gint assign_mysql_version(const char *newval, void *ex_param);
CHASSIS_API gint assign_lastest_query_num(const char *newval, void *ex_param);
CHASSIS_API gint assign_time_threshold(const char *newval, void *ex_param);
CHASSIS_API gint assign_freq_threshold(const char *newval, void *ex_param);
CHASSIS_API gint assign_access_ratio(const char *newval, void *ex_param);
CHASSIS_API gint assign_backend_max_thread_running(const char *newval, void *ex_param);
CHASSIS_API gint assign_thread_running_sleep_delay(const char *newval, void *ex_param);
CHASSIS_API gint assign_auto_filter_flag(const char *newval, void *ex_param);
CHASSIS_API gint assign_manual_filter_flag(const char *newval, void *ex_param);
CHASSIS_API gint assign_blacklist_file(const char *newval, void *ex_param);
CHASSIS_API gint assign_remove_backend_timeout(const char *newval, void *ex_param);
CHASSIS_API gint assign_log_trace_modules(const char *newval, void *ex_param);
CHASSIS_API gint assign_backend_monitor_pwds(const char *newval, void *ex_param);
CHASSIS_API gint assign_db_connect_timeout(const char *newval, void *ex_param);

/* show utils */
CHASSIS_API gchar* show_verbose_shutdown(void *external_param);
CHASSIS_API gchar* show_basedir(void *external_param);
CHASSIS_API gchar* show_plugin_dir(void *external_param);
CHASSIS_API gchar* show_plugins(void *external_param);
CHASSIS_API gchar* show_log_level(void *external_param);
CHASSIS_API gchar* show_log_path(void *external_param);
CHASSIS_API gchar* show_log_bt_on_crash(void *external_param);
CHASSIS_API gchar* show_keepalive(void *external_param);
CHASSIS_API gchar* show_max_open_files(void *external_param);
CHASSIS_API gchar* show_max_connections(void *external_param);
CHASSIS_API gchar* show_long_wait_time(void *external_param);
CHASSIS_API gchar* show_long_query_time(void *external_param);
CHASSIS_API gchar* show_time_range_base(void *external_param);
CHASSIS_API gchar* show_time_stats(void *external_param);
CHASSIS_API gchar* show_event_threads(void *external_param);
CHASSIS_API gchar* show_lua_path(void *external_param);
CHASSIS_API gchar* show_user(void *external_param);
CHASSIS_API gchar* show_instance(void *external_param);
CHASSIS_API gchar* show_wait_timeout(void *external_param);
CHASSIS_API gchar* show_shutdown_timeout(void *external_param);
CHASSIS_API gchar* show_db_connection_idle_timeout(void *external_param);
CHASSIS_API gchar* show_db_connection_max_age(void *external_param);
CHASSIS_API gchar* show_mysql_version(void *external_param);
CHASSIS_API gchar* show_lastest_query_num(void *external_param);
CHASSIS_API gchar* show_time_threshold(void *external_param);
CHASSIS_API gchar* show_freq_threshold(void *external_param);
CHASSIS_API gchar* show_access_ratio(void *external_param);
CHASSIS_API gchar* show_backend_max_thread_running(void *external_param);
CHASSIS_API gchar* show_thread_running_sleep_delay(void *external_param);
CHASSIS_API gchar* show_auto_filter_flag(void *external_param);
CHASSIS_API gchar* show_manual_filter_flag(void *external_param);
CHASSIS_API gchar* show_blacklist_file(void *external_param);
CHASSIS_API gchar* show_remove_backend_timeout(void *external_param);
CHASSIS_API gchar* show_log_trace_modules(void *external_param);
CHASSIS_API gchar* show_version(void *external_param);
CHASSIS_API gchar* show_daemon(void *external_param);
CHASSIS_API gchar* show_assign_backend_monitor_pwds(void *external_param);
CHASSIS_API gchar* show_db_connect_timeout(void *external_param);
CHASSIS_API int save_config(chassis *chas);
CHASSIS_API gboolean opt_match(const char *str, const char *prefix);
CHASSIS_API gint set_raw_int_value(const gchar *newval, gint *param, gint min, gint max);
#endif /* __CHASSIS_OPTIONS_UTILS_H__ */
