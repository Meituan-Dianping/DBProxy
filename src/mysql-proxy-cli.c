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
 
/** @file
 * the user-interface for the MySQL Proxy @see main()
 *
 *  -  command-line handling 
 *  -  config-file parsing
 * 
 *
 * network_mysqld_thread() is the real proxy thread 
 * 
 * @todo move the SQL based help out into a lua script
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>

#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#ifdef _WIN32
#include <process.h> /* getpid() */
#include <io.h>      /* open() */
#else
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h> /* for rusage in wait() */
#endif

#include <glib.h>
#include <gmodule.h>

#ifdef HAVE_LUA_H
#include <lua.h>
#include <stdio.h>
#endif

#ifdef HAVE_VALGRIND_VALGRIND_H
#include <valgrind/valgrind.h>
#endif

#ifndef HAVE_VALGRIND_VALGRIND_H
#define RUNNING_ON_VALGRIND 0
#endif

#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "sys-pedantic.h"

#include "chassis-log.h"
#include "chassis-keyfile.h"
#include "chassis-mainloop.h"
#include "chassis-path.h"
#include "chassis-limits.h"
#include "chassis-filemode.h"
#include "chassis-win32-service.h"
#include "chassis-unix-daemon.h"
#include "chassis-frontend.h"
#include "chassis-options.h"
#include "chassis-options-utils.h"
#include "chassis-filter.h"
#ifdef HAVE_PWD_H
#include <pwd.h>     /* getpwnam() */
#endif

#define GETTEXT_PACKAGE "mysql-proxy"
/**
 * change the owner of directories recursively
 */
gint chown_recursion(const gchar *path, uid_t owner, gid_t group) {
    GError *error = NULL;
    GDir *dir;
    const gchar *filename;
    gchar *pathPlusFilename;
    gint ret = 0;
    ret = chown(path, owner, group);
    if(ret == -1) {
        g_log_dbproxy(g_critical, "chown(%s) failed: %s", path, g_strerror(errno) );
        goto exit;
    }
    dir = g_dir_open(path, 0, &error);
    if(dir == NULL) {
        g_log_dbproxy(g_critical, "g_dir_open(%s) failed: %s", path, error->message );
        goto exit;
    }
    while((filename = g_dir_read_name(dir)) != NULL) {
        pathPlusFilename = g_build_filename(path, filename, (gchar *)NULL);
        if(g_file_test(pathPlusFilename, G_FILE_TEST_IS_REGULAR)) {
            ret =  chown(pathPlusFilename, owner, group);
        } else if(g_file_test(pathPlusFilename, G_FILE_TEST_IS_DIR)) {
            ret = chown_recursion(pathPlusFilename, owner, group);
        }
        g_free(pathPlusFilename);
        if(ret == -1) {
            g_log_dbproxy(g_critical, "chown(%s) failed: %s", pathPlusFilename, g_strerror(errno));
            goto exit;
        }
    }
exit:
    g_clear_error(&error);
    g_dir_close(dir);
    return ret;
}

/**
 * options of the MySQL Proxy frontend
 */
typedef struct {
    int print_version;
    int verbose_shutdown;

    int daemon_mode;
    gchar *user;

    gchar *base_dir;
    int auto_base_dir;

    gchar *default_file;
    GKeyFile *keyfile;

    chassis_plugin *p;
    GOptionEntry *config_entries;

    gchar *pid_file;

    gchar *plugin_dir;
    gchar **plugin_names;

    guint invoke_dbg_on_crash;

#ifndef _WIN32
    /* the --keepalive option isn't available on Unix */
    guint auto_restart;
#endif

    gint max_files_number;

    gint max_connections;

    gint long_wait_time;
    gint long_query_time;
    gint query_response_time_range_base;
    gint query_response_time_stats;

    gint event_thread_count;

    gchar *log_level;
    gchar *log_path;
    gint    log_trace_modules;
    int    use_syslog;

    char *lua_path;
    char *lua_cpath;
    char **lua_subdirs;

    gchar *instance_name;

    gint wait_timeout;

    gint shutdown_timeout;

    gint db_connection_idle_timeout;
    gint db_connection_max_age;

    gchar *mysql_version;

    gint lastest_query_num;
    gint query_filter_time_threshold;
    gdouble query_filter_frequent_threshold;
    gint  access_num_per_time_window;

    gchar *auto_filter_flag;
    gchar *manual_filter_flag;

    gint max_backend_tr;
    gint thread_running_sleep_delay;
    gchar *backend_pwds;

    gchar *blacklist_file;

    gint remove_backend_timeout;
    gchar *sql_log;
    gchar *sql_log_mode_str;
    gint  sql_log_slow_ms;
    gint  sql_log_max_size;
    gint  sql_log_file_num;

    gchar *percentile_switch;
    gint percentile_value;
} chassis_frontend_t;

/**
 * create a new the frontend for the chassis
 */
chassis_frontend_t *chassis_frontend_new(void) {
    chassis_frontend_t *frontend;

    frontend = g_slice_new0(chassis_frontend_t);
    frontend->event_thread_count = 1;
    frontend->max_files_number = 0;
    frontend->wait_timeout = 0;
    frontend->shutdown_timeout = 1;
    frontend->query_response_time_stats = 1;
    frontend->query_response_time_range_base = 2;

    frontend->db_connection_idle_timeout = 3600;
    frontend->db_connection_max_age = 7200;

    frontend->lastest_query_num = 0;
    frontend->query_filter_frequent_threshold = 0.0;
    frontend->query_filter_time_threshold = -1;
    frontend->access_num_per_time_window = 5;
    frontend->auto_filter_flag = 0;
    frontend->manual_filter_flag = 0;

    frontend->max_backend_tr = 0;
    frontend->thread_running_sleep_delay = 1;

    frontend->remove_backend_timeout = 3600;

    frontend->blacklist_file = NULL;

    frontend->percentile_switch = NULL;
    frontend->percentile_value = 95;
    return frontend;
}

/**
 * free the frontend of the chassis
 */
void chassis_frontend_free(chassis_frontend_t *frontend) {
    if (!frontend) return;

    if (frontend->keyfile) g_key_file_free(frontend->keyfile);
    if (frontend->default_file) g_free(frontend->default_file);
    if (frontend->base_dir) g_free(frontend->base_dir);
    if (frontend->user) g_free(frontend->user);
    if (frontend->pid_file) g_free(frontend->pid_file);
    if (frontend->log_level) g_free(frontend->log_level);
    if (frontend->plugin_dir) g_free(frontend->plugin_dir);

    g_strfreev(frontend->plugin_names);

    if (frontend->lua_path) g_free(frontend->lua_path);
    if (frontend->lua_cpath) g_free(frontend->lua_cpath);
    if (frontend->lua_subdirs) g_strfreev(frontend->lua_subdirs);
    if (frontend->instance_name) g_free(frontend->instance_name);

    if (frontend->backend_pwds) g_free(frontend->backend_pwds);

    if (frontend->blacklist_file) g_free(frontend->blacklist_file);
    if (frontend->auto_filter_flag) g_free(frontend->auto_filter_flag);
    if (frontend->manual_filter_flag) g_free(frontend->manual_filter_flag);

    if (frontend->sql_log) g_free(frontend->sql_log);
    if (frontend->sql_log_mode_str) g_free(frontend->sql_log_mode_str);

    if (frontend->percentile_switch) g_free(frontend->percentile_switch);

    g_slice_free(chassis_frontend_t, frontend);
}

/**
 * setup the options of the chassis
 */
int chassis_frontend_set_chassis_options(chassis_frontend_t *frontend, chassis_options_t *opts) {
    chassis_options_add(opts, "verbose-shutdown", 0, 0, G_OPTION_ARG_NONE, &(frontend->verbose_shutdown), "Always log the exit code when shutting down", NULL, NULL, NULL, 0);
    chassis_options_add(opts, "daemon", 0, 0, G_OPTION_ARG_NONE, &(frontend->daemon_mode), "Start in daemon-mode", NULL, NULL, show_daemon, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "user", 0, 0, G_OPTION_ARG_STRING, &(frontend->user), "Run mysql-proxy as user", "<user>", NULL, show_user, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "basedir", 0, 0, G_OPTION_ARG_STRING, &(frontend->base_dir), "Base directory to prepend to relative paths in the config", "<absolute path>", assign_basedir, show_basedir, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "plugin-dir", 0, 0, G_OPTION_ARG_STRING, &(frontend->plugin_dir), "path to the plugins", "<path>", NULL, NULL, ASSIGN_SHOW);
    chassis_options_add(opts, "plugins", 0, 0, G_OPTION_ARG_STRING_ARRAY, &(frontend->plugin_names), "plugins to load", "<name>", NULL, show_plugins, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "log-level", 0, 0, G_OPTION_ARG_STRING, &(frontend->log_level), "log all messages of level ... or higher", "(error|warning|info|message|debug)", assign_log_level, show_log_level, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "log-path", 0, 0, G_OPTION_ARG_STRING, &(frontend->log_path), "log all messages in a path", "<path>", assign_log_path, show_log_path, SHOW_SAVE);
    chassis_options_add(opts, "log-use-syslog", 0, 0, G_OPTION_ARG_NONE, &(frontend->use_syslog), "log all messages to syslog", NULL, NULL, NULL, 0);
    chassis_options_add(opts, "log-backtrace-on-crash", 0, 0, G_OPTION_ARG_NONE, &(frontend->invoke_dbg_on_crash), "try to invoke debugger on crash", NULL, assign_log_bt_on_crash, show_log_bt_on_crash, 0);
    chassis_options_add(opts, "keepalive", 0, 0, G_OPTION_ARG_NONE, &(frontend->auto_restart), "try to restart the proxy if it crashed", NULL, NULL, show_keepalive, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "max-open-files", 0, 0, G_OPTION_ARG_INT, &(frontend->max_files_number), "maximum number of open files (ulimit -n)", NULL, NULL, show_max_open_files, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "max-connections", 0, 0, G_OPTION_ARG_INT, &(frontend->max_connections), "maximum connections", NULL, assign_max_connections, show_max_connections, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "long-wait-time", 0, 0, G_OPTION_ARG_INT, &(frontend->long_wait_time), "if a sync waiting takes longer than the specified time, it will be considered to be long (ms)", NULL, assign_long_wait_time, show_long_wait_time, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "long-query-time", 0, 0, G_OPTION_ARG_INT, &(frontend->long_query_time), "if a query takes longer than the specified time, it will be considered to be long (ms)", NULL, assign_long_query_time, show_long_query_time, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "query-response-time-range-base", 0, 0, G_OPTION_ARG_INT,
                                                &(frontend->query_response_time_range_base), "the logarithm base for the scale of query times' statistics(default: 2)",
                                                NULL, assign_time_range_base, show_time_range_base, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "query-response-time-stats", 0, 0, G_OPTION_ARG_INT, &(frontend->query_response_time_stats),
                                                "whether to enable collection of query times(0: none statistics, 1: slow query statistics, 2: histogram statistics)(default: 1)", NULL,
                                               assign_time_stats, show_time_stats, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "event-threads", 0, 0, G_OPTION_ARG_INT, &(frontend->event_thread_count), "number of event-handling threads (default: 1)", NULL, NULL, show_event_threads, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "lua-path", 0, 0, G_OPTION_ARG_STRING, &(frontend->lua_path), "set the LUA_PATH", "<...>", NULL, NULL, 0);
    chassis_options_add(opts, "lua-cpath", 0, 0, G_OPTION_ARG_STRING, &(frontend->lua_cpath), "set the LUA_CPATH", "<...>", NULL, NULL, 0);
    chassis_options_add(opts, "instance", 0, 0, G_OPTION_ARG_STRING, &(frontend->instance_name), "instance name", "<name>", NULL, show_instance, SHOW_OPTS_PROPERTY);
    chassis_options_add(opts, "wait-timeout", 0, 0, G_OPTION_ARG_INT, &(frontend->wait_timeout), "the number of seconds which dbproxy waits for activity on a connection before closing it (default:0)", NULL,
                                                    assign_wait_timeout, show_wait_timeout, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "shutdown-timeout", 0, 0, G_OPTION_ARG_INT, &(frontend->shutdown_timeout), "the number of seconds which dbproxy waits for activity on a connection before closing it during shutdown process(default:1)", NULL,
                                                    assign_shutdown_timeout, show_shutdown_timeout, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "db-connection-idle-timeout", 0, 0, G_OPTION_ARG_INT, &(frontend->db_connection_idle_timeout), "the number of seconds which dbproxy will close a backend's idle connection after  (default:3600s)", NULL,
                                                    assign_db_connetion_idle_timeout, show_db_connection_idle_timeout, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "db-connection-max-age", 0, 0, G_OPTION_ARG_INT, &(frontend->db_connection_max_age), "the number of seconds which dbproxy will close a backend's connection after  (default:7200s), 0 means no maximum absolute age is enforced", NULL,
                                                    assign_db_connection_max_age, show_db_connection_max_age, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "mysql-version", 0, 0, G_OPTION_ARG_STRING, &(frontend->mysql_version), "the version of the backends  (default:5.5)", NULL, assign_mysql_version, show_mysql_version, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "lastest-query-num", 0, 0, G_OPTION_ARG_INT, &(frontend->lastest_query_num), "number of lastest queries (default:0, don't save lastest queries)", NULL, assign_lastest_query_num, show_lastest_query_num, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "query-filter-time-threshold", 0, 0, G_OPTION_ARG_INT, &(frontend->query_filter_time_threshold), "the threshold of query executing time when adding to filter (default: -1, no threshold)", NULL,
                                                    assign_time_threshold, show_time_threshold, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "query-filter-frequent-threshold", 0, 0, G_OPTION_ARG_DOUBLE, &(frontend->query_filter_frequent_threshold), "the threshold of query executing frequency(access times per-second) when adding to filter (default: = 0)", NULL,
                                                    assign_freq_threshold, show_freq_threshold, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "access-num-per-time-window", 0, 0, G_OPTION_ARG_INT, &(frontend->access_num_per_time_window), "the threshold of query executing times when calculating frequency (default: 5)", NULL,
                                                    assign_access_ratio, show_access_ratio, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "backend-max-thread-running", 0, 0, G_OPTION_ARG_INT, &(frontend->max_backend_tr), "the default value of blacklist's filter flag (default: 0, close)", NULL,
                                                    assign_backend_max_thread_running, show_backend_max_thread_running, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "thread-running-sleep-delay", 0, 0, G_OPTION_ARG_INT, &(frontend->thread_running_sleep_delay), "the thread running wait time_out(ms)", NULL,
                                                    assign_thread_running_sleep_delay, show_thread_running_sleep_delay, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "auto-filter-flag", 0, 0, G_OPTION_ARG_STRING, &(frontend->auto_filter_flag), "the default flag of auto added filter", NULL,
                                                    assign_auto_filter_flag, show_auto_filter_flag, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "manual-filter-flag", 0, 0, G_OPTION_ARG_STRING, &(frontend->manual_filter_flag), "the default flag of manual added filter", NULL,
                                                    assign_manual_filter_flag, show_manual_filter_flag, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "blacklist-file", 0, 0, G_OPTION_ARG_STRING, &(frontend->blacklist_file), "the blacklist file name", NULL,
                                                    assign_blacklist_file, show_blacklist_file, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "remove-backend-timeout", 0, 0, G_OPTION_ARG_INT, &(frontend->remove_backend_timeout), "the number of seconds which dbproxy waits for removing backend success", NULL,
                                                    assign_remove_backend_timeout, show_remove_backend_timeout, ALL_OPTS_PROPERTY);

    chassis_options_add(opts, "log-trace-modules", 0, 0, G_OPTION_ARG_INT, &(frontend->log_trace_modules), "moudles which want to trace the process,"
                                                                                                            " none: 0x00 connection_pool:0x01 event:0x02 sql: 0x04 con_status:0x08 all:0x0F", NULL,
                                                    assign_log_trace_modules, show_log_trace_modules, ALL_OPTS_PROPERTY);

    chassis_options_add(opts, "backend-monitor-pwds", 0, 0, G_OPTION_ARG_STRING, &(frontend->backend_pwds), "the user and password to to check the backends health status", "user:pwd", assign_backend_monitor_pwds, show_assign_backend_monitor_pwds, ALL_OPTS_PROPERTY);
    chassis_options_add(opts, "version", 0, 0, G_OPTION_ARG_INT, &(frontend->print_version), "print dbproxy version", NULL, NULL, show_version, SHOW_OPTS_PROPERTY);

    return 0;   
}

static void sigsegv_handler(int G_GNUC_UNUSED signum) {
    g_on_error_stack_trace(g_get_prgname());
    abort(); /* trigger a SIGABRT instead of just exiting */
}

/**
 * This is the "real" main which is called both on Windows and UNIX platforms.
 * For the Windows service case, this will also handle the notifications and set
 * up the logging support appropriately.
 */
int main_cmdline(int argc, char **argv) {
    chassis *srv = NULL;
#ifdef HAVE_SIGACTION
    static struct sigaction sigsegv_sa;
#endif
    /* read the command-line options */
    GOptionContext *option_ctx = NULL;
    GOptionEntry *main_entries = NULL;
    chassis_frontend_t *frontend = NULL;
    chassis_options_t *opts = NULL;

    GError *gerr = NULL;
    chassis_log *log = NULL;

    /* a little helper macro to set the src-location that we stepped out at to exit */
#define GOTO_EXIT(status) \
    exit_code = status; \
    exit_location = G_STRLOC; \
    goto exit_nicely;

    int exit_code = EXIT_SUCCESS;
    const gchar *exit_location = G_STRLOC;

    if (chassis_frontend_init_glib()) { /* init the thread, module, ... system */
        GOTO_EXIT(EXIT_FAILURE);
    }

    /* start the logging ... to stderr */
    log = chassis_log_new();
    log->min_lvl = G_LOG_LEVEL_MESSAGE; /* display messages while parsing or loading plugins */
    g_log_set_default_handler(chassis_log_func, log);

#ifdef _WIN32
    if (chassis_win32_is_service() && chassis_log_set_event_log(log, g_get_prgname())) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (chassis_frontend_init_win32()) { /* setup winsock */
        GOTO_EXIT(EXIT_FAILURE);
    }
#endif

    /* may fail on library mismatch */
    if (NULL == (srv = chassis_new())) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    srv->log = log; /* we need the log structure for the log-rotation */

    frontend = chassis_frontend_new();
    option_ctx = g_option_context_new("- MySQL Proxy");
    /**
     * parse once to get the basic options like --defaults-file and --version
     *
     * leave the unknown options in the list
     */
    if (chassis_frontend_init_base_options(option_ctx, &argc, &argv, &(frontend->print_version), &(frontend->default_file), &gerr)) {
        g_log_dbproxy(g_critical, "%s", gerr->message);
        g_clear_error(&gerr);
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (frontend->default_file) {
        if (!(frontend->keyfile = chassis_frontend_open_config_file(frontend->default_file, &gerr))) {
            g_log_dbproxy(g_critical, "loading config from '%s' failed: %s", frontend->default_file, gerr->message);
            g_clear_error(&gerr);
            GOTO_EXIT(EXIT_FAILURE);
        }
    }

    /* print the main version number here, but don't exit
     * we check for print_version again, after loading the plugins (if any)
     * and print their version numbers, too. then we exit cleanly.
     */
    if (frontend->print_version) {
        g_print("%s" CHASSIS_NEWLINE, CHASSIS_BUILD_TAG); 
        g_print("  glib2: %d.%d.%d" CHASSIS_NEWLINE, GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);
        g_print("  libevent: %s" CHASSIS_NEWLINE, event_get_version());
        GOTO_EXIT(EXIT_SUCCESS);
    }

    /* add the other options which can also appear in the configfile */
    opts = chassis_options_new();
    chassis_frontend_set_chassis_options(frontend, opts);
    main_entries = chassis_options_to_g_option_entries(opts);
    g_option_context_add_main_entries(option_ctx, main_entries, NULL);

    srv->opts = opts;

    /**
     * parse once to get the basic options 
     *
     * leave the unknown options in the list
     */

    g_option_context_set_help_enabled(option_ctx, TRUE);
    if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
        g_log_dbproxy(g_critical, "%s", gerr->message);
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (frontend->keyfile) {
        if (chassis_keyfile_to_options(frontend->keyfile, "mysql-proxy", main_entries)) {
            GOTO_EXIT(EXIT_FAILURE);
        }
    }

    if (chassis_frontend_init_basedir(argv[0], &(frontend->base_dir))) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    /* basic setup is done, base-dir is known, ... */
    frontend->lua_subdirs = g_new(char *, 2);
    frontend->lua_subdirs[0] = g_strdup("mysql-proxy");
    frontend->lua_subdirs[1] = NULL;

    if (chassis_frontend_init_lua_path(frontend->lua_path, frontend->base_dir, frontend->lua_subdirs)) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (chassis_frontend_init_lua_cpath(frontend->lua_cpath, frontend->base_dir, frontend->lua_subdirs)) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    /* make sure that he max-thread-count isn't negative */
    if (frontend->event_thread_count < 1) {
        g_log_dbproxy(g_critical, "--event-threads has to be >= 1, is %d", frontend->event_thread_count);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->event_thread_count = frontend->event_thread_count;

    if (frontend->wait_timeout < 0) {
        g_log_dbproxy(g_critical, "--wait-timeout has to be >= 0, is %d", frontend->wait_timeout);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->wait_timeout = frontend->wait_timeout;

    if (frontend->shutdown_timeout < 1) {
        g_log_dbproxy(g_critical, "--shutdown-timeout has to be >= 1, is %d", frontend->shutdown_timeout);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->shutdown_timeout = frontend->shutdown_timeout;

    if (frontend->db_connection_idle_timeout < 0) {
        g_log_dbproxy(g_critical, "--db-connection-idle-timeout has to be >= 0, is %d", frontend->db_connection_idle_timeout);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->db_connection_idle_timeout = frontend->db_connection_idle_timeout;

    if (frontend->db_connection_max_age < 0) {
        g_log_dbproxy(g_critical, "--db-connection-max-age has to be >= 0, is %d", frontend->db_connection_max_age);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->db_connection_max_age = frontend->db_connection_max_age;

    if (frontend->mysql_version == NULL
            || strcmp(frontend->mysql_version, "5.5") == 0) {
        srv->my_version = MYSQL_55;
    } else if (frontend->mysql_version == NULL
            || strcmp(frontend->mysql_version, "5.6") == 0) {
        srv->my_version = MYSQL_56;
    } else {
        g_log_dbproxy(g_critical, "--mysql-version has to be 5.6 or 5.5, is %s", frontend->mysql_version);
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (frontend->max_connections < 0) {
        g_log_dbproxy(g_critical, "--max-connections has to be >= 0, is %d", frontend->max_connections);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_max_connections = frontend->max_connections;

    if (frontend->long_wait_time < 0) {
        g_log_dbproxy(g_critical, "--long-wait-time has to be >= 0, is %d", frontend->long_wait_time);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->long_wait_time = frontend->long_wait_time;

    if (frontend->long_query_time < 0) {
        g_log_dbproxy(g_critical, "--long-query-time has to be >= 0, is %d", frontend->long_query_time);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->long_query_time = frontend->long_query_time;


    if (frontend->query_response_time_stats < 0) {
        g_log_dbproxy(g_critical, "--query-response-time-stats has to be >= 0, is %d", frontend->query_response_time_stats);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->query_response_time_stats = frontend->query_response_time_stats;

    if (frontend->query_response_time_stats > 1 && frontend->query_response_time_range_base < 2) {
        g_log_dbproxy(g_critical, "--query-response-time-range-base has to be >= 2, is %d", frontend->query_response_time_range_base);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->query_response_time_range_base = frontend->query_response_time_range_base;

    if (frontend->lastest_query_num < 0) {
        g_log_dbproxy(g_critical, "--lastest-query-num has to be >= 0, is %d", frontend->lastest_query_num);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_reserved->lastest_query_num = frontend->lastest_query_num;

    if (frontend->query_filter_time_threshold < -1) {
        g_log_dbproxy(g_critical, "--query-filter-time-threshold has to be >= -1, is %d", frontend->query_filter_time_threshold);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_reserved->query_filter_time_threshold = frontend->query_filter_time_threshold;

    if (frontend->query_filter_frequent_threshold < 0) {
        g_log_dbproxy(g_critical, "--query-filter-frequent-threshold has to be >= 0, is %f", frontend->query_filter_frequent_threshold);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_reserved->query_filter_frequent_threshold = frontend->query_filter_frequent_threshold;

    if (frontend->access_num_per_time_window < 1) {
        g_log_dbproxy(g_critical, "--freq-access-num-threshold has to be >= 1, is %d", frontend->access_num_per_time_window);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_reserved->access_num_per_time_window = frontend->access_num_per_time_window;

    int flag = 0;
    if (frontend->auto_filter_flag == NULL || strcasecmp(frontend->auto_filter_flag, "OFF") == 0)
        flag = 0;
    else if (strcasecmp(frontend->auto_filter_flag, "ON") == 0)
        flag = 1;
    else
    {
        g_log_dbproxy(g_critical, "--auto-filter-flag has to be on or off, is %s", frontend->auto_filter_flag);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_filter->auto_filter_flag = flag;

    if (frontend->manual_filter_flag == NULL || strcasecmp(frontend->manual_filter_flag, "OFF") == 0)
        flag = 0;
    else if (strcasecmp(frontend->manual_filter_flag, "ON") == 0)
        flag = 1;
    else
    {
        g_log_dbproxy(g_critical, "--manual-filter-flag has to be on or off, is %s", frontend->manual_filter_flag);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->proxy_filter->manual_filter_flag = flag;

    if (frontend->max_backend_tr < 0) {
        g_log_dbproxy(g_critical, "--backend-max-thread-running has to be >= 0, is %d", frontend->max_backend_tr);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->max_backend_tr = frontend->max_backend_tr;

    if (frontend->thread_running_sleep_delay < 1) {
        g_log_dbproxy(g_critical, "--thread-running-sleep-delay has to be >= 1, is %d", frontend->thread_running_sleep_delay);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->thread_running_sleep_delay = frontend->thread_running_sleep_delay;

    if (frontend->remove_backend_timeout < 0) {
        g_log_dbproxy(g_critical, "--remove-backend-timeout has to be >= 0, is %d", frontend->remove_backend_timeout);
        GOTO_EXIT(EXIT_FAILURE);
    }

    /* assign the mysqld part to the */
    network_mysqld_init(srv, frontend->default_file, frontend->remove_backend_timeout); /* starts the also the lua-scope, LUA_PATH and LUA_CPATH have to be set before this being called */

#ifdef HAVE_SIGACTION
    /* register the sigsegv interceptor */

    memset(&sigsegv_sa, 0, sizeof(sigsegv_sa));
    sigsegv_sa.sa_handler = sigsegv_handler;
    sigemptyset(&sigsegv_sa.sa_mask);

    if (frontend->invoke_dbg_on_crash && !(RUNNING_ON_VALGRIND)) {
        sigaction(SIGSEGV, &sigsegv_sa, NULL);
    }
#endif

    /*
     * some plugins cannot see the chassis struct from the point
     * where they open files, hence we must make it available
     */
    srv->base_dir = g_strdup(frontend->base_dir);

    chassis_frontend_init_plugin_dir(&frontend->plugin_dir, srv->base_dir);
    
    /* 
     * these are used before we gathered all the options
     * from the plugins, thus we need to fix them up before
     * dealing with all the rest.
     */
    if (chassis_frontend_init_logdir(frontend->log_path)) {
        GOTO_EXIT(EXIT_FAILURE);
    }
    chassis_resolve_path(srv->base_dir, &frontend->log_path);
    chassis_resolve_path(srv->base_dir, &frontend->plugin_dir);

    /*
     * start the logging and pid
     */
    if (frontend->log_path) {
        if (frontend->instance_name == NULL) {
            gchar *default_file = frontend->default_file;

            gchar *slash = strrchr(default_file, '/');
            if (slash != NULL) ++slash;
            else slash = default_file;

            gchar *dot = strrchr(default_file, '.');
            if (dot != NULL && dot >= slash) frontend->instance_name = g_strndup(slash, dot-slash);
            else frontend->instance_name = g_strdup(slash);
        }
        log->log_filename = g_strdup_printf("%s/%s.log", frontend->log_path, frontend->instance_name);
        frontend->pid_file = g_strdup_printf("%s/%s.pid", frontend->log_path, frontend->instance_name);
        srv->log_path = g_strdup(frontend->log_path);
    }

    log->use_syslog = frontend->use_syslog;

    if (log->log_filename && log->use_syslog) {
        g_log_dbproxy(g_critical, "log-file and log-use-syslog were given, but only one is allowed");
        GOTO_EXIT(EXIT_FAILURE);
    }

    /*
     * we have to drop root privileges in chassis_mainloop() after
     * the plugins opened the ports, so we need the user there
     */
    if (frontend->user) {
    srv->user = g_strdup(frontend->user);
    }

    /**
     * create the path of sql and its parents path if it is necessary
     */
    gchar *sql_path = g_strdup_printf("%s/%s", srv->log_path, SQL_LOG_DIR);
    if(g_mkdir_with_parents(sql_path, S_IRWXU|S_IRWXG|S_IRWXO) != 0) {
        g_log_dbproxy(g_critical, "g_mkdir_with_parents(%s) failed: %s", sql_path, g_strerror(errno));
        g_free(sql_path);
        GOTO_EXIT(EXIT_FAILURE);
    }
    g_free(sql_path);
    
    /*
     * check whether we should drop root privileges
     */
#ifndef _win32
    if (srv->user) {
        struct passwd *user_info;
        uid_t user_id= geteuid();
        /* Don't bother if we aren't superuser */
        if (user_id) {
            g_log_dbproxy(g_warning, "current user is not root, --user is ignored");
        } else {
            if (NULL == (user_info = getpwnam(srv->user))) {
                g_log_dbproxy(g_critical, "unknown user: %s", srv->user);
                return -1;
            }

            /* chown log-path */
            if (-1 == chown_recursion(srv->log_path, user_info->pw_uid, user_info->pw_gid)) {
                g_log_dbproxy(g_critical, "chown_recursion(%s) failed: %s", srv->log_path, g_strerror(errno));
                GOTO_EXIT(EXIT_FAILURE);
            }

            setgid(user_info->pw_gid);
            setuid(user_info->pw_uid);
            g_log_dbproxy(g_debug, "running as user: %s (%d/%d)", srv->user, user_info->pw_uid, user_info->pw_gid);

            /*check the config file can read or write, and the config file dir can execute*/
            gchar *config_file = frontend->default_file;

            /*if the config_file is relative get the real path */
            char resolved_path[PATH_MAX];
            if (g_path_is_absolute(config_file) == FALSE) {
                realpath(config_file, resolved_path);
                config_file = resolved_path;
            }

            gchar *config_dir = g_path_get_dirname(config_file);

            if (-1 == faccessat(0, config_file, R_OK | W_OK, AT_EACCESS)
                    || -1 == faccessat(0, config_dir, X_OK | W_OK, AT_EACCESS)) {
                g_free(config_dir);
                g_log_dbproxy(g_critical, "%s don't have correct privilege for the user %s set in the config file, config file should have read write privileges and the config file dir should have execute and write privilege.", config_file, srv->user);
                GOTO_EXIT(EXIT_FAILURE);
            }

            g_free(config_dir);

            g_log_dbproxy(g_message, "running as user: %s (%s/%s)", srv->user, user_info->pw_uid, user_info->pw_gid);
        } 
    }
#endif  

    if (log->log_filename && FALSE == chassis_log_open(log)) {
        g_log_dbproxy(g_critical, "can't open log-file '%s': %s", log->log_filename, g_strerror(errno));
        GOTO_EXIT(EXIT_FAILURE);
    }

    /* handle log-level after the config-file is read, just in case it is specified in the file */
    if (frontend->log_level) {
        if (0 != chassis_log_set_level(log, frontend->log_level)) {
            g_log_dbproxy(g_critical, "--log-level=... failed, level '%s' is unknown ", frontend->log_level);
            GOTO_EXIT(EXIT_FAILURE);
        }
    } else {
        /* if it is not set, use "critical" as default */
        log->min_lvl = G_LOG_LEVEL_CRITICAL;
    }

    if (frontend->log_trace_modules < 0) {
        g_log_dbproxy(g_critical, "log-trace-modules has to be >= 0, is %d", frontend->log_trace_modules);
        GOTO_EXIT(EXIT_FAILURE);
    }
    srv->log->log_trace_modules = frontend->log_trace_modules;

    /*
     * the MySQL Proxy should load 'admin' and 'proxy' plugins
     */
    if (!frontend->plugin_names) {
        frontend->plugin_names = g_new(char *, 3);

        frontend->plugin_names[0] = g_strdup("admin");
        frontend->plugin_names[1] = g_strdup("proxy");
        frontend->plugin_names[2] = NULL;
    }

    if (chassis_frontend_load_plugins(srv->modules, frontend->plugin_dir, frontend->plugin_names)) {
        GOTO_EXIT(EXIT_FAILURE);
    }

    if (frontend->backend_pwds && strlen(frontend->backend_pwds) > 0) {
        gchar *user = NULL, *pwd = NULL;
        gchar *cur_pwd_info = g_strdup(frontend->backend_pwds);
        gchar *tmp_for_free = cur_pwd_info;

        if ((user = strsep(&cur_pwd_info, ":")) != NULL && (pwd = strsep(&cur_pwd_info, ":")) != NULL) {
            if (network_backends_set_monitor_pwd(srv->backends, user, pwd, TRUE) != 0) {
                g_log_dbproxy(g_critical, "set monitor pwd failed");
            }
         } else {
                g_log_dbproxy(g_critical, "set monitor pwd failed: invalid value %s", frontend->backend_pwds);
         }
         g_free(tmp_for_free);
     }
/*
    if (chassis_frontend_init_plugins(srv->modules, option_ctx, &argc, &argv, frontend->keyfile, "mysql-proxy", srv->base_dir, &gerr)) {
        g_log_dbproxy(g_critical, "%s", gerr->message);
        g_clear_error(&gerr);
        GOTO_EXIT(EXIT_FAILURE);
    }
*/
    /* if we only print the version numbers, exit and don't do any more work */
    if (frontend->print_version) {
        chassis_frontend_print_lua_version();
        chassis_frontend_print_plugin_versions(srv->modules);
        GOTO_EXIT(EXIT_SUCCESS);
    }

    /* we know about the options now, lets parse them */
    g_option_context_set_ignore_unknown_options(option_ctx, FALSE);

    /* handle unknown options */
    if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
        if (gerr->domain == G_OPTION_ERROR && gerr->code == G_OPTION_ERROR_UNKNOWN_OPTION) {
            g_log_dbproxy(g_critical, "%s (use --help to show all options)", gerr->message);
        } else {
            g_log_dbproxy(g_critical, "%s (code = %d, domain = %s)", gerr->message, gerr->code, g_quark_to_string(gerr->domain));
        }
        
        GOTO_EXIT(EXIT_FAILURE);
    }
/*
    g_option_context_free(option_ctx);
    option_ctx = NULL;
*/
    /* after parsing the options we should only have the program name left */
    if (argc > 1) {
        g_log_dbproxy(g_critical, "unknown option: %s", argv[1]);
        GOTO_EXIT(EXIT_FAILURE);
    }
    
#ifndef _WIN32  
    signal(SIGPIPE, SIG_IGN);

    if (frontend->daemon_mode) {
        srv->daemon_mode = 1;
        chassis_unix_daemonize();
    }

    if (frontend->auto_restart) {
        int child_exit_status = EXIT_SUCCESS; /* forward the exit-status of the child */
        int ret = chassis_unix_proc_keepalive(&child_exit_status, frontend->pid_file);

        if (ret > 0) {
            /* the agent stopped */

            exit_code = child_exit_status;
            goto exit_nicely;
        } else if (ret < 0) {
            GOTO_EXIT(EXIT_FAILURE);
        } else {
            /* we are the child, go on */
        }

       srv->auto_restart = 1;
    }
#endif
    if (frontend->keyfile) {
        g_key_file_free(frontend->keyfile);
        frontend->keyfile = NULL;
    }
    if (frontend->default_file) {
        if (!(frontend->keyfile = chassis_frontend_open_config_file(frontend->default_file, &gerr))) {
            g_log_dbproxy(g_critical, "loading config from '%s' failed: %s", frontend->default_file, gerr->message);
            g_clear_error(&gerr);
            GOTO_EXIT(EXIT_FAILURE);
        }
    }
    if (chassis_frontend_init_plugins(srv->modules, option_ctx, &argc, &argv, frontend->keyfile, "mysql-proxy", srv->base_dir, &gerr)) {
        g_log_dbproxy(g_critical, "%s", gerr->message);
        g_clear_error(&gerr);
        GOTO_EXIT(EXIT_FAILURE);
    }
    g_option_context_free(option_ctx);
    option_ctx = NULL;

    if (frontend->pid_file) {
        if (0 != chassis_frontend_write_pidfile(frontend->pid_file, &gerr)) {
            g_log_dbproxy(g_critical, "%s", gerr->message);
            g_clear_error(&gerr);

            GOTO_EXIT(EXIT_FAILURE);
        }
    }

    // we need instance name that read by lua, may be use in lua log and ...
    srv->instance_name = g_strdup(frontend->instance_name);

    /* the message has to be _after_ the g_option_content_parse() to 
     * hide from the output if the --help is asked for
     */
    g_log_dbproxy(g_message, "%s started - instance: %s", PACKAGE_STRING, srv->instance_name); /* add tag to the logfile (after we opened the logfile) */

#ifdef _WIN32
    if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_RUNNING, 0);
#endif

    /*
     * we have to drop root privileges in chassis_mainloop() after
     * the plugins opened the ports, so we need the user there
     */
    srv->user = g_strdup(frontend->user);

    if (frontend->max_files_number) {
        if (0 != chassis_fdlimit_set(frontend->max_files_number)) {
            g_log_dbproxy(g_critical, "setting fdlimit = %d failed: %s (%d)", frontend->max_files_number, g_strerror(errno), errno);
            GOTO_EXIT(EXIT_FAILURE);
        }
        srv->max_files_number = frontend->max_files_number;
    } else {
        srv->max_files_number = chassis_fdlimit_get();
    }
    g_log_dbproxy(g_debug, "max open file-descriptors = %"G_GINT64_FORMAT, chassis_fdlimit_get());

    if (frontend->blacklist_file)
    {
        chassis_resolve_path(srv->base_dir, &frontend->blacklist_file);
        srv->proxy_filter->blacklist_file = g_strdup(frontend->blacklist_file);
    }
    else
    {
        srv->proxy_filter->blacklist_file = g_strdup("conf/blacklist.dat");
        chassis_resolve_path(srv->base_dir, &srv->proxy_filter->blacklist_file);
    }

    /*check the black list file dir exist execute and write*/
    gchar *blacklist_file = srv->proxy_filter->blacklist_file;

    /*if the blacklist file is relative get the real path */
    char blacklist_file_real[PATH_MAX];
    if (g_path_is_absolute(blacklist_file) == FALSE) {
        realpath(blacklist_file, blacklist_file_real);
        blacklist_file = blacklist_file_real;
    }

    gchar *blacklist_file_dir = g_path_get_dirname(blacklist_file);

    if ( -1 == faccessat(0, blacklist_file_dir, F_OK | X_OK | W_OK, AT_EACCESS)) {
        if (ENOENT == errno) {//black file dir is not exit.
            g_log_dbproxy(g_warning, "dir of %s don't exit ,the blacklist file dir should exit, otherwise save blacklist should be fail", blacklist_file);

        }
        else if (EACCES == errno) {//black file dir don't have correct privilges.
            g_log_dbproxy(g_warning, "%s don't have correct privilege for the user %s set in the blacklist file, blacklist file dir should have execute and write privilege, otherwise save blacklist should be fail", blacklist_file, srv->user);
        }
    }
    g_free(blacklist_file_dir);

    load_sql_filter_from_file(srv->proxy_filter);

    if (srv->proxy_reserved->access_num_per_time_window > 0 &&
                srv->proxy_reserved->query_filter_frequent_threshold > 1e-3)
    {
        gdouble freq_threshold = srv->proxy_reserved->query_filter_frequent_threshold;
        gint gap_threshold = srv->proxy_reserved->access_num_per_time_window;

        set_freq_time_windows(srv->proxy_reserved, freq_threshold, gap_threshold);
    }

    if (chassis_mainloop(srv)) {
        /* looks like we failed */
        g_log_dbproxy(g_critical, "Failure from chassis_mainloop. Shutting down");
        GOTO_EXIT(EXIT_FAILURE);
    }

exit_nicely:

#ifdef _WIN32
    if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_STOP_PENDING, 0);
#endif

    if (gerr) g_error_free(gerr);
    if (option_ctx) g_option_context_free(option_ctx);
    if (srv) chassis_free(srv);
    if (opts) chassis_options_free(opts);
    if (main_entries) chassis_frontend_options_free(main_entries);

    chassis_log_free(log);
    
#ifdef _WIN32
    if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_STOPPED, 0);
#endif

#ifdef HAVE_SIGACTION
    /* reset the handler */
    sigsegv_sa.sa_handler = SIG_DFL;
    if (frontend->invoke_dbg_on_crash && !(RUNNING_ON_VALGRIND)) {
        sigaction(SIGSEGV, &sigsegv_sa, NULL);
    }
#endif
    chassis_frontend_free(frontend);    

    return exit_code;
}

/**
 * On Windows we first look if we are started as a service and 
 * set that up if appropriate.
 * We eventually fall down through to main_cmdline, even on Windows.
 */
int main(int argc, char **argv) {
#ifdef WIN32_AS_SERVICE
    return main_win32(argc, argv, main_cmdline);
#else
    return main_cmdline(argc, argv);
#endif
}

