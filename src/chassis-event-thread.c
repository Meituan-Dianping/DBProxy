/* $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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

#include <glib.h>
#include <errno.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h> /* for write() */
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h> /* for SOCK_STREAM and AF_UNIX/AF_INET */
#endif

#ifdef WIN32
#include <winsock2.h>
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <io.h> /* for write, read, _pipe etc */
#include <fcntl.h>
#undef WIN32_LEAN_AND_MEAN
#endif

#include <event.h>
#include <glib.h>

#include "chassis-event-thread.h"
#include "network-mysqld-packet.h"

#define C(x) x, sizeof(x) - 1
#ifndef WIN32
#define closesocket(x) close(x)
#endif

__thread gint cur_thid;


/**
 * add a event asynchronously
 *
 * the event is added to the global event-queue and a fd-notification is sent allowing any
 * of the event-threads to handle it
 *
 * @see network_mysqld_con_handle()
 */
void chassis_event_add(network_mysqld_con* client_con) {        //���߳�ִ�У�ping�����̣߳�ʹ����ν���״̬��
    chassis* chas = client_con->srv;

    // choose a event thread
    static guint last_thread = 1;
    if (last_thread > chas->event_thread_count) last_thread = 1;
    chassis_event_thread_t *thread = chas->threads->pdata[last_thread];

    ++last_thread;

    if (TRACE_CON_STATUS(client_con->srv->log->log_trace_modules)) {
        gchar *msg = g_strdup_printf("connect add to event thread(%d)'s async queue", last_thread - 1);
            CON_MSG_HANDLE(g_message, client_con, msg);
            g_free(msg);
    }

    g_async_queue_push(thread->event_queue, client_con);
    g_atomic_pointer_add(&(thread->thread_status_var.thread_stat[THREAD_STAT_EVENT_WAITING]), 1);
    if (write(thread->notify_send_fd, "", 1) != 1) g_log_dbproxy(g_error, "pipes - write error: %s", g_strerror(errno));
}


void chassis_event_add_by_thread(chassis_event_thread_t* thread, struct event* ev, int timeout_s, int timeout_us)
{
    struct event_base *event_base = thread->event_base;
    event_base_set(event_base, ev);

    if (timeout_s < 0 || timeout_us < 0 || (timeout_s == 0 && timeout_us == 0))
    {
        event_add(ev, NULL);
    }
    else
    {
        struct timeval tm = {timeout_s, timeout_us};
        event_add(ev, &tm);
    }
}

void chassis_event_add_self(chassis* chas, struct event* ev, int timeout_s, int timeout_us) {   //�����߳�ִ�У�RETRY���¼����·��뱾�̵߳�event_base��
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);

    chassis_event_add_by_thread(thread, ev, timeout_s, timeout_us);
}

/**
 * add a event to the current thread 
 *
 * needs event-base stored in the thread local storage
 *
 * @see network_connection_pool_lua_add_connection()
 */
void chassis_event_add_local(chassis *chas, struct event *ev) {     //�����̻߳�����߳�ִ�У�����������ӵĳ�ʱ�¼�EV_READ��������ٳ�ʱ����
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);
    struct event_base *event_base = thread->event_base;

    g_assert(event_base); /* the thread-local event-base has to be initialized */

    event_base_set(event_base, ev);
    event_add(ev, NULL);
}

void chassis_event_handle(int G_GNUC_UNUSED event_fd, short G_GNUC_UNUSED events, void* user_data) {
    chassis_event_thread_t* thread = user_data;

    char ping[1];
    if (read(thread->notify_receive_fd, ping, 1) != 1) g_log_dbproxy(g_error, "pipes - read error");

    network_mysqld_con* client_con = g_async_queue_try_pop(thread->event_queue);
    if (client_con != NULL) {
        g_atomic_pointer_add(&(thread->thread_status_var.thread_stat[THREAD_STAT_EVENT_WAITING]), -1);
        chassis_event_add_connection(NULL, thread, client_con);

        network_mysqld_con_handle(-1, 0, client_con);
    }
}

/**
 * create the data structure for a new event-thread
 */
chassis_event_thread_t *chassis_event_thread_new(guint index) {
    chassis_event_thread_t *thread = g_new0(chassis_event_thread_t, 1);

    thread->index = index;

    thread->event_queue = g_async_queue_new();

    g_rw_lock_init(&thread->connection_lock);
    thread->connection_list = NULL;

    thread->exit_phase = EVENT_THREAD_NORMAL;
    return thread;
}

/**
 * free the data-structures for a event-thread
 *
 * joins the event-thread, closes notification-pipe and free's the event-base
 */
void chassis_event_thread_free(chassis_event_thread_t *thread) {
    if (!thread) return;

    if (thread->thr) g_thread_join(thread->thr);

    if (thread->notify_receive_fd != -1) {
        event_del(&(thread->notify_fd_event));
        closesocket(thread->notify_receive_fd);
    }
    if (thread->notify_send_fd != -1) {
        closesocket(thread->notify_send_fd);
    }

    /* we don't want to free the global event-base */
    if (thread->thr != NULL && thread->event_base) event_base_free(thread->event_base);

    network_mysqld_con* con = NULL;
    while ((con = g_async_queue_try_pop(thread->event_queue))) {
        network_mysqld_con_free(con);
    }
    g_atomic_pointer_set(&(thread->thread_status_var.thread_stat[THREAD_STAT_EVENT_WAITING]), 0);
    g_async_queue_unref(thread->event_queue);

    g_rw_lock_clear(&thread->connection_lock);
    if (thread->connection_list != NULL)
    {
        g_list_free_full(thread->connection_list, (GDestroyNotify)network_mysqld_con_free);
    }

    g_free(thread);
}

/**
 * setup the notification-fd of a event-thread
 *
 * all event-threads listen on the same notification pipe
 *
 * @see chassis_event_handle()
 */ 
int chassis_event_threads_init_thread(chassis_event_thread_t *thread, chassis *chas) {
    thread->event_base = event_base_new();
    thread->chas = chas;

    int fds[2];
    if (pipe(fds)) {
        int err;
        err = errno;
        g_log_dbproxy(g_error, "evutil_socketpair() failed: %s (%d)", 
                g_strerror(err),
                err);
    }
    thread->notify_receive_fd = fds[0];
    thread->notify_send_fd = fds[1];

    event_set(&(thread->notify_fd_event), thread->notify_receive_fd, EV_READ | EV_PERSIST, chassis_event_handle, thread);
    event_base_set(thread->event_base, &(thread->notify_fd_event));
    event_add(&(thread->notify_fd_event), NULL);

    return 0;
}

static void chassis_event_thread_update_conn_status(chassis_event_thread_t *thread)
{
    network_mysqld_con  *conn = NULL;
    GList               *gl_conn = NULL;
    network_mysqld_con_lua_t *st = NULL;

    g_assert(thread != NULL);

    gl_conn = thread->connection_list;
    while (gl_conn) {
        conn = gl_conn->data;
        st = conn->plugin_con_state;

        if (chassis_is_shutdown_normal() &&
                g_atomic_int_get(&conn->conn_status.exit_phase) != CON_EXIT_TX) {
            g_atomic_int_set(&conn->conn_status.exit_begin_time, time(NULL));
            g_atomic_int_set(&conn->conn_status.exit_phase, CON_EXIT_TX);
        }

        if (g_atomic_int_get(&conn->conn_status.exit_phase) == CON_EXIT_KILL ||
                g_atomic_int_get(&conn->conn_status.exit_phase) == CON_EXIT_TX) {
                /*|| (st != NULL && st->backend != NULL && IS_BACKEND_WAITING_EXIT(st->backend)))*/
            struct event *ev = NULL;
            gchar *event_msg = NULL;
            int pending = event_pending(&conn->client->event, EV_READ|EV_WRITE|EV_TIMEOUT, NULL);
            if (pending) {
                ev = &conn->client->event;
                event_msg = "client";
            } else {
                pending = event_pending(&conn->server->event, EV_READ|EV_WRITE|EV_TIMEOUT, NULL);
                ev = &conn->server->event;
                event_msg = "server";
            }
            if (pending != 0) {
                /*
                 * 1 stands for the times of calling callback function after manual active event,
                 * this parameter has been obsoleted at libevent-2.0.
                 */
                g_log_dbproxy(g_debug, "pending %s's %d event", event_msg, pending);
                event_active(ev, pending, 1);
            }
        }
        gl_conn = g_list_next(gl_conn);
    }
}

/**
 * event-handler thread
 *
 */
void *chassis_event_thread_loop(chassis_event_thread_t *thread) {
    cur_thid = thread->index;
    gboolean has_no_active_con = FALSE;

    /**
     * check once a second if we shall shutdown the proxy
     */
    while (!chassis_is_shutdown_immediate() && (!chassis_is_shutdown_normal() || !has_no_active_con)) {
        struct timeval timeout;
        int r;

        /* check exit status */
        if (chassis_is_shutdown_normal()) {
            if (thread->connection_nums == 0) {
            has_no_active_con = TRUE;
                g_log_dbproxy(g_debug, "no connection in thread %d", thread->index);
                continue;
            }
        }

        if (!chassis_is_shutdown_immediate()) {
            chassis_event_thread_update_conn_status(thread);
        }

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        g_assert(event_base_loopexit(thread->event_base, &timeout) == 0);

        r = event_base_dispatch(thread->event_base);

        if (r == -1) {
#ifdef WIN32
            errno = WSAGetLastError();
#endif
            if (errno == EINTR) continue;
            g_log_dbproxy(g_critical, "leaving chassis_event_thread_loop early, errno != EINTR was: %s (%d)", g_strerror(errno), errno);
            break;
        }
    }

    g_log_dbproxy(g_message, "work thread %d will exit", thread->index);
    g_atomic_int_set(&thread->exit_phase, EVENT_THREAD_EXITED);

    return NULL;
}

/**
 * event-handler thread
 *
 */
void *chassis_mainloop_thread_loop(chassis_event_thread_t *thread) {
    cur_thid = thread->index;
    gboolean is_all_work_thread_exit = FALSE;
    g_assert(thread->index == 0);

    /**
     * check once a second if we shall shutdown the proxy
     */
    while (!chassis_is_shutdown() || !is_all_work_thread_exit) {
        struct timeval timeout;
        int r;

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        g_assert(event_base_loopexit(thread->event_base, &timeout) == 0);

        r = event_base_dispatch(thread->event_base);

        if (r == -1) {
#ifdef WIN32
            errno = WSAGetLastError();
#endif
            if (errno == EINTR) continue;
            g_log_dbproxy(g_critical, "leaving chassis_event_thread_loop early, errno != EINTR was: %s (%d)", g_strerror(errno), errno);
            break;
        }

        if (chassis_is_shutdown()) {
                gint i = 1;
                for (; i < thread->chas->threads->len; i++) {
                    chassis_event_thread_t *event_thread = g_ptr_array_index(thread->chas->threads, i);
                    if (g_atomic_int_get(&event_thread->exit_phase) != EVENT_THREAD_EXITED) break;
                }
                if (i == thread->chas->threads->len) {
                    is_all_work_thread_exit = TRUE;
                }
            }
    }

    g_log_dbproxy(g_message, "main loop thread will exit");

    return NULL;
}

/**
 * start all the event-threads 
 *
 * starts all the event-threads that got added by chassis_event_threads_add()
 *
 * @see chassis_event_threads_add
 */
void chassis_event_threads_start(GPtrArray *threads) {
    guint i;

    g_log_dbproxy(g_message, "starting %d threads", threads->len - 1);

    for (i = 1; i < threads->len; i++) { /* the 1st is the main-thread and already set up */
        chassis_event_thread_t *thread = threads->pdata[i];
        GError *gerr = NULL;

        thread->thr = g_thread_try_new("event thread", (GThreadFunc)chassis_event_thread_loop, thread, &gerr);
        if (gerr) {
            g_log_dbproxy(g_critical, "%s", gerr->message);
            g_error_free(gerr);
            gerr = NULL;
        }
    }
}

network_connection_pool* chassis_event_thread_pool(network_backend_t* backend) {
    return g_ptr_array_index(backend->pools, cur_thid);
}


gboolean chassis_add_connection(chassis *chas) {
    gint current_connections = 0;
    gint current_max_connectted = 0;

    current_connections = g_atomic_int_add(&chas->proxy_connections, 1);
    if (chas->proxy_max_connections > 0 && current_connections + 1 > chas->proxy_max_connections)
    {
        return FALSE;
    }

    current_max_connectted = chas->proxy_max_used_connections;
    if (current_connections + 1 > current_max_connectted)
    {
        // 忽略结果，因为如果失败则肯定被其他线程设置成至少等于当前current_connections的值
        g_atomic_int_compare_and_exchange(&chas->proxy_max_used_connections, current_max_connectted, current_connections + 1);
    }

    return TRUE;
}

void chassis_dec_connection(chassis *chas) {
    g_atomic_int_dec_and_test(&chas->proxy_connections);
}

void chassis_event_add_connection(chassis *chas, chassis_event_thread_t* thread, network_mysqld_con *client_con) {
    if (NULL == thread){
        g_assert(chas != NULL);
        thread = g_ptr_array_index(chas->threads, cur_thid);
    }

    g_assert(client_con != NULL && thread != NULL);

    g_rw_lock_writer_lock(&(thread->connection_lock));

    if (((++thread->cur_max_con_idx) & CON_IDX_MASK) == 0) {
        thread->cur_max_con_idx = CON_PRE_FIRST_ID;
    }
    client_con->con_id = MAKE_CON_ID(thread->index, thread->cur_max_con_idx);

    thread->connection_list = g_list_append(thread->connection_list, client_con);
    thread->connection_nums++;
    g_rw_lock_writer_unlock(&thread->connection_lock);
}

void chassis_event_remove_connection(chassis *chas, network_mysqld_con *client_con) {
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);

    g_assert(client_con != NULL);

    g_rw_lock_writer_lock(&thread->connection_lock);
    thread->connection_list = g_list_remove(thread->connection_list, client_con);
    thread->connection_nums--;
    g_rw_lock_writer_unlock(&thread->connection_lock);
}

thread_status_var_t* chassis_event_thread_get_status(chassis *chas)
{
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);
    return &thread->thread_status_var;
}

guint chassis_event_get_threadid() {
    return cur_thid;
}

void chassis_event_thread_wait_start(chassis *chas, network_mysqld_wait_event_t event_type) {
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);

    g_assert(event_type < WAIT_EVENT_END);

    thread->thread_status_var.wait_event_status.event_type = event_type;
    thread->thread_status_var.wait_event_status.wait_start = chassis_get_rel_microseconds();
}

guint64 chassis_event_thread_wait_end(chassis *chas, network_mysqld_wait_event_t event_type) {
    chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, cur_thid);
    guint64 long_wait_time = 0;

    g_assert(event_type < WAIT_EVENT_END);

    if (thread->thread_status_var.wait_event_status.event_type == event_type) {
        guint wait_time = chassis_get_rel_microseconds() - thread->thread_status_var.wait_event_status.wait_start;
        wait_event_stat_t *p_wait_event_stat = &thread->thread_status_var.wait_event_stat[event_type];

        p_wait_event_stat->waits++;
        p_wait_event_stat->total_wait_time += wait_time;

        if (p_wait_event_stat->min_wait_time == 0 || wait_time < p_wait_event_stat->min_wait_time) {
            p_wait_event_stat->min_wait_time = wait_time;
        }

        if (wait_time > p_wait_event_stat->max_wait_time) {
            p_wait_event_stat->max_wait_time = wait_time;
        }

        if (chas->long_wait_time > 0 && wait_time > chas->long_wait_time * 1000)
        {
            p_wait_event_stat->longer_waits++;
            p_wait_event_stat->total_longer_wait_time += wait_time;

            long_wait_time = wait_time;
        }

        thread->thread_status_var.wait_event_status.event_type = WAIT_EVENT_END;
    }

    return long_wait_time;
}
