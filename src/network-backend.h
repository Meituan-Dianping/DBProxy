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
 

#ifndef _BACKEND_H_
#define _BACKEND_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define PWD_SUCCESS     0
#define ERR_USER_EXIST      1
#define ERR_USER_NOT_EXIST  1
#define ERR_PWD_ENCRYPT     2
#define ERR_PWD_DECRYPT     2

#define ADD_PWD             1
#define ADD_ENPWD           2
#define REMOVE_PWD          3
#define ADD_USER_HOST       4
#define REMOVE_USER_HOST    5
#define ADD_BACKENDS        6
#define REMOVE_BACKENDS     7
#define BACKEND_STATE       8
#define ADD_ADMIN_HOSTS     9
#define REMOVE_ADMIN_HOSTS  10
#define ALTTER_ADMIN_PWDS   11

#define WHITELIST_USER  "%"
#define USER_IDENT      "@"
#define IPS_SEP         "|"
#define IP_END          "%"
#define BACKENDS_SEP    IPS_SEP
#define COMMA_SEP       ","
#define ITEM_SPLIT COMMA_SEP

#include "network-conn-pool.h"
#include "network-exports.h"

typedef enum { 
    BACKEND_STATE_UNKNOWN, 
    BACKEND_STATE_UP, 
    BACKEND_STATE_PENDING,
    BACKEND_STATE_DOWN,
    BACKEND_STATE_OFFLINING,
    BACKEND_STATE_OFFLINE,
    BACKEND_STATE_REMOVING
} backend_state_t;

typedef enum { 
    BACKEND_TYPE_UNKNOWN, 
    BACKEND_TYPE_RW, 
    BACKEND_TYPE_RO
} backend_type_t;


typedef struct {
    network_address *addr;
   
    volatile backend_state_t state;   /**< UP or DOWN */
    backend_type_t type;     /**< ReadWrite or ReadOnly */

    guint64  state_since;    /**< timestamp of the last state-change */
    guint    offline_timeout;    /* offline timeout */
    volatile guint connected_clients; /**< number of open connections to this backend for SQF */

//  network_connection_pool *pool; /**< the pool of open connections */
    GPtrArray *pools;

    GString *uuid;           /**< the UUID of the backend */
    guint weight;

    GString *slave_tag;

    gint        thread_running;
    GRWLock     backend_lock;
} network_backend_t;

#define IS_BACKEND_OFFLINE(bk) (g_atomic_int_get(&bk->state) == BACKEND_STATE_OFFLINE)
#define IS_BACKEND_OFFLINING(bk) (g_atomic_int_get(&bk->state) == BACKEND_STATE_OFFLINING)
#define IS_BACKEND_REMOVING(bk) (g_atomic_int_get(&bk->state) == BACKEND_STATE_REMOVING)
#define IS_BACKEND_UP(bk)       (g_atomic_int_get(&bk->state) == BACKEND_STATE_UP)
#define IS_BACKEND_DOWN(bk)     (g_atomic_int_get(&bk->state) == BACKEND_STATE_DOWN)
#define IS_BACKEND_UNKNOWN(bk)  (g_atomic_int_get(&bk->state) == BACKEND_STATE_UNKNOWN)
#define IS_BACKEND_PENDING(bk)  (g_atomic_int_get(&bk->state) == BACKEND_STATE_PENDING)
#define IS_BACKEND_WAITING_EXIT(bk)  (IS_BACKEND_OFFLINING(bk) || IS_BACKEND_REMOVING(bk))

#define SET_BACKEND_STATE(bk, status)   g_atomic_int_set(&((bk)->state), (status));

NETWORK_API network_backend_t *network_backend_new();
NETWORK_API void network_backend_free(network_backend_t *b);


typedef struct {
    guint max_weight;
    guint cur_weight;
    guint next_ndx;
} g_wrr_poll;

typedef struct admin_user_info {
    GRWLock     admin_user_lock;
    gchar       *name;
    gchar       *password;
    GPtrArray   *admin_hosts;
} admin_user_info;

typedef struct network_backends_tag{
    g_wrr_poll *wrr_poll;
    GPtrArray *backends;
} network_backends_tag;

typedef struct {
    GPtrArray *backends;
    GHashTable *tag_backends;
    network_backends_tag *def_backend_tag;
    GRWLock    backends_lock;   /*remove lock*/
    g_wrr_poll *global_wrr;
    guint event_thread_count;
    gchar *default_file;
    GHashTable **ip_table;
    gint *ip_table_index;
    GPtrArray *raw_ips;

    gchar *monitor_user;
    gchar *monitor_pwd;
    gchar *monitor_encrypt_pwd;

    GPtrArray   *raw_pwds;            /* save the text format user info, migrate from pwd_table. */
    GHashTable  *pwd_table;          /* save the users info, include username, hashed_password, user_limits_ip */
    GRWLock     user_mgr_lock;
    volatile guint remove_backend_timeout;
    admin_user_info *au;
} network_backends_t;

NETWORK_API network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file);
NETWORK_API void network_backends_free(network_backends_t *);
NETWORK_API int network_backends_add(network_backends_t *backends, gchar *address, backend_type_t type);
NETWORK_API int network_backends_remove(network_backends_t *bs, network_backend_t *backend);
NETWORK_API int network_backends_addclient(network_backends_t *backends, gchar *address);
NETWORK_API int network_backends_removeclient(network_backends_t *backends, gchar *address);
NETWORK_API int network_backends_addpwd(network_backends_t *backends, const gchar *user, const gchar *pwd, gboolean is_encrypt);
NETWORK_API int network_backends_removepwd(network_backends_t *backends, const gchar *user);
NETWORK_API int network_backends_check(network_backends_t *backends);
NETWORK_API network_backend_t * network_backends_get(network_backends_t *backends, guint ndx);
NETWORK_API guint network_backends_count(network_backends_t *backends);
NETWORK_API gint network_backends_set_monitor_pwd(network_backends_t *bs, const gchar *user, const gchar *pwd, gboolean is_encrypt);

NETWORK_API g_wrr_poll *g_wrr_poll_new();
NETWORK_API void g_wrr_poll_init(g_wrr_poll *wrr_poll, GPtrArray *backends);
NETWORK_API void g_wrr_poll_free(g_wrr_poll *global_wrr);

NETWORK_API char *decrypt(const char *in);
NETWORK_API char *encrypt_dbproxy(const char *in);

typedef struct {
    gchar *username;
    gchar *encrypt_pwd;
    gchar *user_hosts;
    gchar *backends;
} raw_user_info;

NETWORK_API raw_user_info *raw_user_info_new(const gchar *username, const gchar *encrypt_pwd, const gchar *user_hosts, gchar *backends);

/* user info mgr */
typedef struct user_info_hval {
    GString     *hashed_password;
    GPtrArray   *user_hosts;
    GPtrArray *backends_tag;
    gint        user_tag_max_weight;
} user_info_hval;


NETWORK_API user_info_hval *user_info_hval_new(GString *hashed_passwd);
NETWORK_API GString *get_hash_passwd(GHashTable *pwd_table, gchar *username, GRWLock *user_mgr_lock);
NETWORK_API gboolean check_user_host(GHashTable *pwd_table, gchar *username, gchar *client_ip, GRWLock *user_mgr_lock);
NETWORK_API int user_hosts_handle(network_backends_t *bs, const gchar *user, const gchar *raw_user_hosts, gint type);
NETWORK_API int user_backends_handle(network_backends_t *bs, const gchar *user, const gchar *user_backends, gint type);

NETWORK_API network_backends_tag * get_user_backends(network_backends_t *bs, GHashTable *pwd_table, gchar *username, gchar *backend_tag, GRWLock *user_mgr_lock);
NETWORK_API gint alter_slave_weight(network_backends_t *bs, gint idx, gint weight);
NETWORK_API gint add_slave_tag(network_backends_t *bs, gchar *tagname,gchar *idxs );
NETWORK_API gint remove_slave_tag(network_backends_t *bs, gchar *tagname,gchar *idxs );

NETWORK_API admin_user_info *admin_user_info_new();
NETWORK_API void admin_user_info_free(admin_user_info *au);
NETWORK_API gint admin_user_host_add(admin_user_info *au, const gchar *host);
NETWORK_API gint admin_user_host_remove(admin_user_info *au, const gchar *host);
NETWORK_API gint admin_user_host_check(admin_user_info *au, const gchar *client_ip);
NETWORK_API gint admin_user_user_update(admin_user_info *au, const gchar *user, const gchar *pwd);
NETWORK_API GString *admin_user_hosts_show(admin_user_info *au);

NETWORK_API gchar *get_real_ip(gchar *token);

#endif /* _BACKEND_H_ */
