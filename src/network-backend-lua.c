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
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len
#define C_S(x) x, strlen(x)

#include "network-backend.h"
#include "network-mysqld.h"
#include "network-conn-pool-lua.h"
#include "network-backend-lua.h"
#include "network-address-lua.h"
#include "network-mysqld-lua.h"

/**
 * get the info about a backend
 *
 * proxy.backend[0].
 *   connected_clients => clients using this backend
 *   address           => ip:port or unix-path of to the backend
 *   state             => int(BACKEND_STATE_UP|BACKEND_STATE_DOWN) 
 *   type              => int(BACKEND_TYPE_RW|BACKEND_TYPE_RO) 
 *
 * @return nil or requested information
 * @see backend_state_t backend_type_t
 */
static int proxy_backend_get(lua_State *L) {
    network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("connected_clients"))) {
        lua_pushinteger(L, backend->connected_clients);
    } else if (strleq(key, keysize, C("dst"))) {
        network_address_lua_push(L, backend->addr);
    } else if (strleq(key, keysize, C("state"))) {
        lua_pushinteger(L, backend->state);
    } else if (strleq(key, keysize, C("type"))) {
        lua_pushinteger(L, backend->type);
    } else if (strleq(key, keysize, C("uuid"))) {
        if (backend->uuid->len) {
            lua_pushlstring(L, S(backend->uuid));
        } else {
            lua_pushnil(L);
        }
    } else if (strleq(key, keysize, C("weight"))) {
        lua_pushinteger(L, backend->weight);
    } else if (strleq(key, keysize, C("tag"))) {
        if (backend->slave_tag && backend->slave_tag->len) {
            lua_pushlstring(L, S(backend->slave_tag));
    } else {
        lua_pushnil(L);
    }
    } else if (strleq(key, keysize, C("threads_running"))) {
        lua_pushinteger(L, backend->thread_running);
    } else if (strleq(key, keysize, C("hostname"))) {
               if (0 >= backend->addr->hostname->len) {
                        lua_pushstring(L, "");
                } else {
                        lua_pushlstring(L, S(backend->addr->hostname));
                }
    }
    else {
        lua_pushnil(L);
    }

    return 1;
}

static int proxy_backend_set(lua_State *L) {
    network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("uuid"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);

            g_string_assign_len(backend->uuid, s, s_len);
        } else if (lua_isnil(L, -1)) {
            g_string_truncate(backend->uuid, 0);
        } else {
            return luaL_error(L, "proxy.global.backends[...].%s has to be a string", key);
        }
    } else {
        return luaL_error(L, "proxy.global.backends[...].%s is not writable", key);
    }
    return 1;
}

int network_backend_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_backend_get },
        { "__newindex", proxy_backend_set },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}

/**
 * get proxy.global.backends[ndx]
 *
 * get the backend from the array of mysql backends.
 *
 * @return nil or the backend
 * @see proxy_backend_get
 */
static int proxy_backends_get(lua_State *L) {
    network_backend_t *backend; 
    network_backend_t **backend_p;

    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
    int backend_ndx = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
    
    /* check that we are in range for a _int_ */
    if (NULL == (backend = network_backends_get(bs, backend_ndx))) {
        lua_pushnil(L);

        return 1;
    }

    backend_p = lua_newuserdata(L, sizeof(backend)); /* the table underneath proxy.global.backends[ndx] */
    *backend_p = backend;

    network_backend_lua_getmetatable(L);
    lua_setmetatable(L, -2);

    return 1;
}

static int proxy_clients_get(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    int index = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
    gchar *ip = g_ptr_array_index(raw_ips, index);
    lua_pushlstring(L, ip, strlen(ip));
    return 1;
}

static int proxy_pwds_get(lua_State *L) {
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    gint i = 0;

    g_assert(strleq(key, keysize, C("pwds")));

    lua_newtable(L);
    g_rw_lock_reader_lock(&bs->user_mgr_lock);

    for (i = 0; i < bs->raw_pwds->len; i++)
    {
        raw_user_info *rwi = g_ptr_array_index(bs->raw_pwds, i);

        lua_pushnumber(L, i);
        lua_newtable(L);

        lua_pushstring(L, "username");
        lua_pushstring(L, rwi->username);
        lua_settable(L,-3);

        lua_pushstring(L, "password");
        if (rwi->encrypt_pwd) {
            lua_pushstring(L, rwi->encrypt_pwd);
        } else {
            lua_pushstring(L, "");
        }
        lua_settable(L,-3);

        lua_pushstring(L, "hosts");
        if (rwi->user_hosts != NULL) {
            lua_pushstring(L, rwi->user_hosts);
        } else {
            lua_pushstring(L, "");
        }
        lua_settable(L,-3);

        lua_pushstring(L, "backends");
        if (rwi->backends != NULL) {
            lua_pushstring(L, rwi->backends);
        } else {
            lua_pushstring(L, "");
        }
        lua_settable(L,-3);

        lua_pushstring(L, "type");
        lua_pushstring(L, "proxy");
    lua_settable(L, -3);

        lua_settable(L, -3);
    }

    g_rw_lock_reader_unlock(&bs->user_mgr_lock);

    /* admin user */
    lua_pushnumber(L, i + 1);
    lua_newtable(L);

    lua_pushstring(L, "username");
    lua_pushlstring(L, C_S(bs->au->name));
    lua_settable(L,-3);

    lua_pushstring(L, "password");
    gchar *admin_en_password = encrypt_dbproxy((const gchar *)bs->au->password);
    lua_pushlstring(L, C_S(admin_en_password));
    g_free(admin_en_password);
    lua_settable(L,-3);

    lua_pushstring(L, "hosts");
    g_rw_lock_reader_lock(&bs->au->admin_user_lock);
    GString *admin_hosts = admin_user_hosts_show(bs->au);
    g_rw_lock_reader_unlock(&bs->au->admin_user_lock);
    if (admin_hosts == NULL) {
        lua_pushlstring(L, C(""));
    } else {
        lua_pushlstring(L, S(admin_hosts));
        g_string_free(admin_hosts, TRUE);
    }
    lua_settable(L, -3);

    lua_pushstring(L, "backends");
    lua_pushstring(L, "");
    lua_settable(L, -3);

    lua_pushstring(L, "type");
    lua_pushstring(L, "admin");
    lua_settable(L,-3);

    lua_settable(L, -3);

    return 1;
}

/**
 * set proxy.global.backends.addslave
 *
 * add slave server into mysql backends
 *
 * @return nil or the backend
 */
static int proxy_backends_set(lua_State *L) {
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("addslave"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_add(bs, address, BACKEND_TYPE_RO);
        g_free(address);
    } else if (strleq(key, keysize, C("addmaster"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_add(bs, address, BACKEND_TYPE_RW);
        g_free(address);
    } else if (strleq(key, keysize, C("addclient"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_addclient(bs, address);
        g_free(address);
    } else if (strleq(key, keysize, C("removeclient"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_removeclient(bs, address);
        g_free(address);
    } else if (strleq(key, keysize, C("alterweight"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        gint ndx = -1;
        gint weight = -1;
        gchar *dx = strrchr(address, ':');
        weight = atoi(dx+1);
        *dx = '\0';
        ndx = atoi(address);
        alter_slave_weight(bs, ndx, weight);
        g_free(address);
    } else if (strleq(key, keysize, C("addslavetag"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        gchar *ndxs = strrchr(address,':');
        *ndxs = '\0';
        add_slave_tag(bs, address, ndxs+1);
        g_free(address);
    } else if (strleq(key, keysize, C("removeslavetag"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        gchar *ndxs = strrchr(address,':');
        *ndxs = '\0';
        remove_slave_tag(bs, address, ndxs+1);
        g_free(address);
    } else {
        return luaL_error(L, "proxy.global.backends.%s is not writable", key);
    }
    return 1;
}

static int proxy_backends_len(lua_State *L) {
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
    lua_pushinteger(L, network_backends_count(bs));
    return 1;
}

static int proxy_clients_len(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    lua_pushinteger(L, raw_ips->len);
    return 1;
}

static int proxy_pwds_len(lua_State *L) {
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);

    g_rw_lock_reader_lock(&bs->user_mgr_lock);
    lua_pushinteger(L, bs->raw_pwds->len);
    g_rw_lock_reader_unlock(&bs->user_mgr_lock);
    return 1;
}

static int proxy_clients_exist(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    const gchar *client = lua_tostring(L, -1);
    guint i;
    for (i = 0; i < raw_ips->len; ++i) {
        if (strcmp(client, g_ptr_array_index(raw_ips, i)) == 0) {
            lua_pushinteger(L, 1);
            return 1;
        }
    }
    lua_pushinteger(L, 0);
    return 1;
}

static gboolean proxy_pwds_exist(network_backends_t *bs, const gchar *user) {
    GHashTable *ht = bs->pwd_table;

    g_rw_lock_reader_lock(&bs->user_mgr_lock);
    if (g_hash_table_lookup(ht, user) != NULL) {
        g_rw_lock_reader_unlock(&bs->user_mgr_lock);
            return TRUE;
        }

    g_rw_lock_reader_unlock(&bs->user_mgr_lock);
    return FALSE;
}

static int proxy_backends_users(network_backends_t *bs, gint type, const gchar *user_info, const gchar *user) {

    g_assert(user_info != NULL || user != NULL);
    gboolean is_user_exist = proxy_pwds_exist(bs, user);
    int ret = -1;

    switch (type) {
    case ADD_PWD:
        if (is_user_exist) {
            ret = ERR_USER_EXIST;
            g_log_dbproxy(g_warning, "add pwd %s failed, user %s is already known", user, user);
        } else {
            ret = network_backends_addpwd(bs, user, user_info, FALSE);
        }
        break;

    case ADD_ENPWD:
        if (is_user_exist) {
            ret = ERR_USER_EXIST;
            g_log_dbproxy(g_warning, "add enpwd %s:%s failed, user %s is already known", user, user_info, user);
        } else {
            ret = network_backends_addpwd(bs, user, user_info, TRUE);
        }
        break;

    case REMOVE_PWD:
        if (!is_user_exist) {
            ret = ERR_USER_NOT_EXIST;
            g_log_dbproxy(g_warning, "remove pwd %s failed, user %s is not exist", user, user);
        } else {
            ret = network_backends_removepwd(bs, user);
        }
        break;

    case ADD_USER_HOST:
        if (!is_user_exist) {
            g_log_dbproxy(g_warning, "add user hosts %s failed, user %s is not exist", user_info, user);
            ret = ERR_USER_NOT_EXIST;
        } else {
            user_hosts_handle(bs, user, user_info, ADD_USER_HOST);
            g_log_dbproxy(g_warning, "add user hosts %s for %s", user_info, user);
            ret = 0;
        }
        break;

    case REMOVE_USER_HOST:
        if (!is_user_exist) {
            g_log_dbproxy(g_warning, "remove user hosts %s failed, user %s is not exist", user_info, user);
            ret = ERR_USER_NOT_EXIST;
        } else {
            user_hosts_handle(bs, user, user_info, REMOVE_USER_HOST);
            g_log_dbproxy(g_warning, "remove user hosts %s from user %s", user_info, user);
            ret = 0;
        }
        break;
    case ADD_BACKENDS:
        if (!is_user_exist) {
            g_log_dbproxy(g_warning, "add backend %s failed, user %s is not exist", user_info, user);
            ret = ERR_USER_NOT_EXIST;
        } else {
            user_backends_handle(bs, user, user_info, ADD_BACKENDS);
            g_log_dbproxy(g_warning, "add backend %s for %s", user_info, user);
            ret = 0;
        }
        break;

    case REMOVE_BACKENDS:
        if (!is_user_exist) {
            g_log_dbproxy(g_warning, "remove user backend %s failed, user %s is not exist", user_info, user);
            ret = ERR_USER_NOT_EXIST;
        } else {
            user_backends_handle(bs, user, user_info, REMOVE_BACKENDS);
            g_log_dbproxy(g_warning, "remove user backend %s from user %s", user_info, user);
            ret = 0;
        }
        break;
    case ADD_ADMIN_HOSTS:
        {
            gchar *token = NULL;
            gchar *cur_user_info = g_strdup(user_info);
            gchar *p_for_free = cur_user_info;

            g_rw_lock_writer_lock(&bs->au->admin_user_lock);
            while ((token = strsep(&cur_user_info, ITEM_SPLIT)) != NULL) {
                gchar *real_ip = get_real_ip(g_strstrip(token));
 
            if (NULL == real_ip) continue;
  
                if (strlen(real_ip) > 0) {
                    admin_user_host_add(bs->au, real_ip);
                }

                if (real_ip) { g_free(real_ip); }
            }
            g_rw_lock_writer_unlock(&bs->au->admin_user_lock);
            g_free(p_for_free);

            ret = 0;
            break;
        }
    case REMOVE_ADMIN_HOSTS:
        {
            gchar *token = NULL;
            gchar *cur_user_info = g_strdup(user_info);
            gchar *p_for_free = cur_user_info;

            g_rw_lock_writer_lock(&bs->au->admin_user_lock);
            while ((token = strsep(&cur_user_info, ITEM_SPLIT)) != NULL) {
                gchar *striped_token = g_strstrip(token);
                admin_user_host_remove(bs->au, striped_token);
            }
            g_rw_lock_writer_unlock(&bs->au->admin_user_lock);
            g_free(p_for_free);
            ret = 0;
            break;
        }
    case ALTTER_ADMIN_PWDS:
        {
            gchar *striped_token = g_strdup(user_info);
            ret = admin_user_user_update(bs->au, user, striped_token);
            g_free(striped_token);
            break;
        }
    default:
        g_assert_not_reached();
    }

    return ret;
}


static int proxy_backend_state(network_backends_t *bs, guint timeout, guint index, guint state_type)
{
    guint ret = 0;
    network_backend_t* b = bs->backends->pdata[index];

    if (!b) {
        g_log_dbproxy(g_warning, "%s backend failed, backend %d is not exist",
                            (state_type == BACKEND_STATE_REMOVING) ? "removing" :
                                (state_type == BACKEND_STATE_OFFLINING ? "offlining" : "online" ),
                                index);
        return 1;
    }

    if (IS_BACKEND_UP(b) && state_type <= BACKEND_STATE_UP) {
        return 0;
    } else if (IS_BACKEND_OFFLINING(b) && state_type < BACKEND_STATE_OFFLINING) {
        return 0;
    } else if (IS_BACKEND_OFFLINE(b) && (state_type != BACKEND_STATE_REMOVING &&
                                         state_type != BACKEND_STATE_UNKNOWN)) {
        return 0;
    } else if (IS_BACKEND_REMOVING(b) && state_type < BACKEND_STATE_REMOVING) {
        return 0;
    }

    if (state_type == BACKEND_STATE_REMOVING ||
                state_type == BACKEND_STATE_OFFLINING) {
        gint64 offline_timeout = 0;

        if (timeout == 0) {
            offline_timeout = g_atomic_int_get(&bs->remove_backend_timeout);
        } else {
            offline_timeout = timeout;
        }

        g_rw_lock_writer_lock(&b->backend_lock);
        b->state_since = time(NULL);
        b->offline_timeout = offline_timeout;
        g_rw_lock_writer_unlock(&b->backend_lock);
    }

    SET_BACKEND_STATE(b, state_type);
    g_log_dbproxy(g_message, "%s backend %s success", (state_type == BACKEND_STATE_REMOVING) ? "removing" :
                                        (state_type == BACKEND_STATE_OFFLINING ? "offlining" : "online" ),
                                        b->addr->name->str);

    return ret;
}

static int proxy_backends_call(lua_State *L)
{
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
    guint type  = lua_tointeger(L, -1);
    gint ret = 0;

    if (type == BACKEND_STATE) {
        ret = proxy_backend_state(bs, lua_tointeger(L, -2)/*timeout*/,
                                            lua_tointeger(L, -3)/*bk_id*/,
                                            lua_tointeger(L, -4)/*state type */);
    } else {
        ret = proxy_backends_users(bs, type, lua_tostring(L, -2)/*pwd*/, lua_tostring(L, -3)/*user*/);
    }

    lua_pushinteger(L, ret);
    return 1;
}

int network_backends_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_backends_get },
        { "__newindex", proxy_backends_set },
        { "__len", proxy_backends_len },
        { "__call", proxy_backends_call },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}

int network_clients_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_clients_get },
        { "__len", proxy_clients_len },
        { "__call", proxy_clients_exist },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}

int network_pwds_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_pwds_get },
        { "__len", proxy_pwds_len },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}
