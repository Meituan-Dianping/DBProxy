/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.

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

#include <stdlib.h> 
#include <string.h>
#include <openssl/evp.h>
#include <glib.h>

#include "network-mysqld-packet.h"
#include "network-backend.h"
#include "chassis-plugin.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

static void user_info_hval_free(user_info_hval *hval);
static gint delete_sorted_array(GPtrArray *str_array, const char *target, int (*compar)(const void *, const void *));
static int strcmp_pp(const void *s1, const void *s2);
static void concatenate_str_array(GPtrArray *str_array, gchar **result, gchar *sep);

static gint remove_user_tag_backend(network_backends_t *bs, gchar *tagname);
static gint delete_backend_tagname(network_backends_t *bs, network_backends_tag *tag_backends,
                                    network_backend_t *backend, GString *tag_string);
static gint g_wrr_poll_update(network_backends_t *bs, network_backends_tag *tag_backends);


network_backend_t *network_backend_new(guint event_thread_count) {
    network_backend_t *b = g_new0(network_backend_t, 1);

    b->pools = g_ptr_array_new();
    guint i;
    for (i = 0; i <= event_thread_count; ++i) {
        network_connection_pool* pool = network_connection_pool_new();
        g_ptr_array_add(b->pools, pool);
    }

    b->uuid = g_string_new(NULL);
    b->slave_tag = NULL;
    b->addr = network_address_new();

    b->thread_running = 0;

    g_rw_lock_init(&b->backend_lock);

    return b;
}

void network_backend_free(network_backend_t *b) {
    if (!b) return;

    guint i;
    for (i = 0; i < b->pools->len; ++i) {
        network_connection_pool* pool = g_ptr_array_index(b->pools, i);
        network_connection_pool_free(pool);
    }
    g_ptr_array_free(b->pools, FALSE);

    if (b->addr)     network_address_free(b->addr);
    if (b->uuid)     g_string_free(b->uuid, TRUE);
    if (b->slave_tag)     g_string_free(b->slave_tag, TRUE);

    g_rw_lock_clear(&b->backend_lock);
    g_free(b);
}

raw_user_info *
raw_user_info_new(const gchar *username, const gchar *encrypt_pwd, const gchar *user_hosts, gchar *backends) {
    raw_user_info *rwi = NULL;

    g_assert(username != NULL);

    rwi = g_new0(raw_user_info, 1);
    rwi->username = g_strdup(username);
    if (encrypt_pwd) {
        rwi->encrypt_pwd = g_strdup(encrypt_pwd);
    }
    if (user_hosts) {
        rwi->user_hosts = g_strdup(user_hosts);
    }
    if (backends) {
        rwi->backends = g_strdup(backends);
    }

    return rwi;
}

static void
raw_user_info_free(raw_user_info *rwi)
{
    if (rwi == NULL) return;

    if (rwi->username) g_free(rwi->username);
    if (rwi->encrypt_pwd) g_free(rwi->encrypt_pwd);
    if (rwi->user_hosts) g_free(rwi->user_hosts);
    if (rwi->backends) g_free(rwi->backends);

    g_free(rwi);
}


static network_backends_tag *tag_backends_new() {
    network_backends_tag *tag_backends =  g_new0(network_backends_tag, 1);

    tag_backends->backends = g_ptr_array_new();
    tag_backends->wrr_poll = g_wrr_poll_new();

    return tag_backends;
}

static void tag_backends_free(network_backends_tag *backends) {
    if (backends->backends != NULL) {
        // network_backend_t在释放bs->backends数组时释放
        g_ptr_array_free(backends->backends, TRUE);
    }

    if (backends->wrr_poll != NULL) {
        g_wrr_poll_free(backends->wrr_poll);
    }

    g_free(backends);
}

static void tag_backends_insert(network_backends_t *bs, gchar *tag, network_backend_t *backend) {
    network_backends_tag *tag_backends = NULL;

    g_assert(bs != NULL && tag != NULL && backend != NULL);

    tag_backends = g_hash_table_lookup(bs->tag_backends, tag);
    if (tag_backends == NULL) {
        tag_backends = tag_backends_new();
        g_ptr_array_add(tag_backends->backends, backend);
        g_hash_table_insert(bs->tag_backends, g_strdup(tag), tag_backends);
    } else {
        g_ptr_array_add(tag_backends->backends, backend);
    }

    g_wrr_poll_update(bs, tag_backends);

    return;
}


network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file) {
    network_backends_t *bs;

    bs = g_new0(network_backends_t, 1);

    bs->backends = g_ptr_array_new();
    bs->tag_backends = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (GDestroyNotify)tag_backends_free);
    g_rw_lock_init(&bs->backends_lock); /*remove lock*/
    bs->def_backend_tag = tag_backends_new();

    bs->event_thread_count = event_thread_count;
    bs->default_file = g_strdup(default_file);
    bs->raw_ips = g_ptr_array_new_with_free_func(g_free);

    bs->raw_pwds = g_ptr_array_new_with_free_func((GDestroyNotify)raw_user_info_free);
    bs->pwd_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (GDestroyNotify)user_info_hval_free);
    g_rw_lock_init(&bs->user_mgr_lock);
    bs->au = admin_user_info_new();
    return bs;
}

g_wrr_poll *g_wrr_poll_new() {
    g_wrr_poll *global_wrr;

    global_wrr = g_new0(g_wrr_poll, 1);

    global_wrr->max_weight = 0;
    global_wrr->cur_weight = 0;
    global_wrr->next_ndx = 0;
    
    return global_wrr;
}

void g_wrr_poll_init(g_wrr_poll *wrr_poll, GPtrArray *backends) {
    guint i = 0;

    g_assert(wrr_poll != NULL);
    g_assert(backends != NULL);

    for(i = 0; i < backends->len; ++i) {
        network_backend_t* backend = (network_backend_t*)g_ptr_array_index(backends, i);
        if (backend == NULL) continue;
        if (wrr_poll->max_weight < backend->weight) {
            wrr_poll->max_weight = backend->weight;
            wrr_poll->cur_weight = backend->weight;
        }
    }
}

void g_wrr_poll_free(g_wrr_poll *global_wrr) {
    g_free(global_wrr);
}

void network_backends_free(network_backends_t *bs) {
    gsize i;

    if (!bs) return;

    g_rw_lock_writer_lock(&bs->backends_lock);  /*remove lock*/
    for (i = 0; i < bs->backends->len; i++) {
        network_backend_t *backend = bs->backends->pdata[i];
        
        network_backend_free(backend);
    }
    if (bs->tag_backends != NULL) {
        g_hash_table_remove_all(bs->tag_backends);
        g_hash_table_destroy(bs->tag_backends);
    }
    g_rw_lock_writer_unlock(&bs->backends_lock);    /*remove lock*/

    g_ptr_array_free(bs->backends, TRUE);
    g_rw_lock_clear(&bs->backends_lock);    /*remove lock*/

    tag_backends_free(bs->def_backend_tag);

    g_free(bs->default_file);

    g_ptr_array_free(bs->raw_ips, TRUE);

    if (bs->monitor_user) g_free(bs->monitor_user);
    if (bs->monitor_pwd) g_free(bs->monitor_pwd);
    if (bs->monitor_encrypt_pwd) g_free(bs->monitor_encrypt_pwd);

    if (bs->raw_pwds != NULL) g_ptr_array_free(bs->raw_pwds, TRUE);
    if (bs->pwd_table != NULL) {
        g_hash_table_remove_all(bs->pwd_table);
        g_hash_table_destroy(bs->pwd_table);
    }
    g_rw_lock_clear(&bs->user_mgr_lock);
    admin_user_info_free(bs->au);
    g_free(bs);
}

void copy_key(guint *key, guint *value, GHashTable *table) {
    guint *new_key = g_new0(guint, 1);
    *new_key = *key;
    g_hash_table_add(table, new_key);
}

int network_backends_addclient(network_backends_t *bs, gchar *address) {
    /* reserved for compatibility. */
    return 0;
}

static char *
encrypt(const char *in) {
    EVP_CIPHER_CTX ctx;
    const EVP_CIPHER *cipher = EVP_rc4();
    unsigned char key[] = "590b6dee278c9d";
    unsigned char inter[512] = {};
    int inl = 0, interl = 0, len = 0;

    g_assert(in != NULL);

    //1. DESﾼￓￃￜ
    EVP_CIPHER_CTX_init(&ctx);
    if (EVP_EncryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1)  {
        EVP_CIPHER_CTX_cleanup(&ctx);
        return NULL;
    }

    inl = strlen(in);
    if (EVP_EncryptUpdate(&ctx, inter, &interl, in, inl) != 1) {
        EVP_CIPHER_CTX_cleanup(&ctx);
        return NULL;
    }

    len = interl;
    if (EVP_EncryptFinal_ex(&ctx, inter+len, &interl) != 1) {
        EVP_CIPHER_CTX_cleanup(&ctx);
        return NULL;
    }
    len += interl;
    EVP_CIPHER_CTX_cleanup(&ctx);

    //2. Base64ﾱ￠ￂ￫
    EVP_ENCODE_CTX ectx;
    EVP_EncodeInit(&ectx);

    char *out = g_malloc0(512);
    int outl = 0;

    EVP_EncodeUpdate(&ectx, out, &outl, inter, len);
    len = outl;
    EVP_EncodeFinal(&ectx, out+len, &outl);
    len += outl;

    if (out[len-1] == 10) out[len-1] = '\0';
    return out;
}

char *encrypt_dbproxy(const char *in) {
    return encrypt(in);
}

char *decrypt(const char *in) {    //1. Base64ﾽ￢ￂ￫
        EVP_ENCODE_CTX dctx;
        EVP_DecodeInit(&dctx);

    int inl = strlen(in);
    unsigned char inter[512] = {};
    int interl = 0;
 
    if (EVP_DecodeUpdate(&dctx, inter, &interl, in, inl) == -1) return NULL;
    int len = interl;
    if (EVP_DecodeFinal(&dctx, inter+len, &interl) != 1) return NULL;
        len += interl;

        //2. DESﾽ￢ￂ￫
    EVP_CIPHER_CTX ctx;
    EVP_CIPHER_CTX_init(&ctx);
    const EVP_CIPHER *cipher = EVP_rc4();

    unsigned char key[] = "590b6dee278c9d";
    if (EVP_DecryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1) return NULL;

    char *out = g_malloc0(512);
    int outl = 0;

    if (EVP_DecryptUpdate(&ctx, out, &outl, inter, len) != 1) {
        g_free(out);
        return NULL;
    }
    len = outl;
    if (EVP_DecryptFinal_ex(&ctx, out+len, &outl) != 1) {
        g_free(out);
        return NULL;
    }
    len += outl;

    EVP_CIPHER_CTX_cleanup(&ctx);

    out[len] = '\0';
    return out;
}

int network_backends_addpwd(network_backends_t *bs, const gchar *user, const gchar *pwd, gboolean is_encrypt) {
    GString *hashed_password = g_string_new(NULL);
    user_info_hval  *user_hval = NULL;
    raw_user_info   *rwi = NULL;
    gchar           *show_pwd = NULL;

    if (is_encrypt) {
        gchar *decrypt_pwd = decrypt(pwd);
        if (decrypt_pwd == NULL) {
            g_log_dbproxy(g_critical, "failed to decrypt %s", pwd);
            return ERR_PWD_DECRYPT;
        }
        network_mysqld_proto_password_hash(hashed_password, decrypt_pwd, strlen(decrypt_pwd));
        g_free(decrypt_pwd);

        show_pwd = g_strdup(pwd);
    } else {
        gchar *encrypt_pwd = encrypt(pwd);
        if (encrypt_pwd == NULL) {
            g_log_dbproxy(g_critical, "failed to encrypt %s", pwd);
            return ERR_PWD_ENCRYPT;
        }

        network_mysqld_proto_password_hash(hashed_password, pwd, strlen(pwd));

        show_pwd = encrypt_pwd;
    }

    rwi = raw_user_info_new(user, show_pwd, NULL, NULL);
    g_free(show_pwd);

    user_hval = user_info_hval_new(hashed_password);

    g_rw_lock_writer_lock(&bs->user_mgr_lock);
    if (g_hash_table_lookup(bs->pwd_table, user) == NULL) {
    g_ptr_array_add(bs->raw_pwds, rwi);
    g_hash_table_insert(bs->pwd_table, g_strdup(user), user_hval);
    } else {
        raw_user_info_free(rwi);
        user_info_hval_free(user_hval);
    }
    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    g_log_dbproxy(g_message, "add %s pwd for %s success", is_encrypt ? "ENCRYPT" : "NOENCRYPT",  user);

    return PWD_SUCCESS;
}

int network_backends_removeclient(network_backends_t *bs, gchar *address) {
    /* reserved for compatibility. */
            return 0;
        }

int network_backends_removepwd(network_backends_t *bs, const gchar *user) {
    guint i;

    g_rw_lock_writer_lock(&bs->user_mgr_lock);
    for (i = 0; i < bs->raw_pwds->len; ++i) {
        raw_user_info *rwi = g_ptr_array_index(bs->raw_pwds, i);

        if (g_strcmp0(user, rwi->username) == 0) {
            g_ptr_array_remove_index(bs->raw_pwds, i);
            g_hash_table_remove(bs->pwd_table, user);
            g_rw_lock_writer_unlock(&bs->user_mgr_lock);

            g_log_dbproxy(g_message, "remove pwd  %s success", user);

            return PWD_SUCCESS;
        }
    }
    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    return ERR_USER_NOT_EXIST;
}

void append_key(guint *key, guint *value, GString *str) {
    g_string_append_c(str, ',');
    guint sum = *key;

    g_string_append_printf(str, "%u", sum & 0x000000FF);

    guint i;
    for (i = 1; i <= 3; ++i) {
        sum >>= 8;
        g_string_append_printf(str, ".%u", sum & 0x000000FF);
    }
}

gint network_backends_set_monitor_pwd(network_backends_t *bs, const gchar *user, const gchar *pwd, gboolean is_encrypt) {
    gchar *decrypt_pwd = NULL;
    gchar *encrypt_pwd = NULL;

    if (is_encrypt) {
        decrypt_pwd = decrypt(pwd);
        if (decrypt_pwd == NULL) {
            g_log_dbproxy(g_critical, "failed to decrypt %s", pwd);
            return ERR_PWD_DECRYPT;
        }

        encrypt_pwd = g_strdup(pwd);
    } else {
        encrypt_pwd = encrypt(pwd);
        if (encrypt_pwd == NULL) {
            g_log_dbproxy(g_critical, "failed to encrypt %s", pwd);
            return ERR_PWD_ENCRYPT;
        }

        decrypt_pwd = g_strdup(pwd);
    }

    g_rw_lock_writer_lock(&bs->user_mgr_lock);
    if (bs->monitor_user != NULL) g_free(bs->monitor_user);
    bs->monitor_user = g_strdup(user);
    if (bs->monitor_pwd != NULL) g_free(bs->monitor_pwd);
    bs->monitor_pwd = decrypt_pwd;
    if (bs->monitor_encrypt_pwd != NULL) g_free(bs->monitor_encrypt_pwd);
    bs->monitor_encrypt_pwd = encrypt_pwd;
    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    g_log_dbproxy(g_message, "set monitor pwd %s:%s success", user, pwd);

    return PWD_SUCCESS;
}

void network_backends_remove(network_backends_t *bs, network_backend_t *backend) {
    network_backends_tag *tag_backends = NULL;

    g_assert(bs != NULL && backend != NULL);

    g_rw_lock_writer_lock(&bs->backends_lock);

    if (backend->slave_tag != NULL) {
        tag_backends = g_hash_table_lookup(bs->tag_backends, backend->slave_tag->str);
        g_assert(tag_backends != NULL);

        g_ptr_array_remove(tag_backends->backends, backend);
        if (tag_backends->backends->len == 0) {
            remove_user_tag_backend(bs, backend->slave_tag->str);
            g_hash_table_remove(bs->tag_backends, backend->slave_tag->str);
            tag_backends = NULL;
        }
    } else {
        tag_backends = bs->def_backend_tag;
        g_ptr_array_remove(tag_backends->backends, backend);
    }

    if (tag_backends != NULL) {
        g_wrr_poll_update(bs, tag_backends);
    }

    g_ptr_array_remove(bs->backends, backend);
    network_backend_free(backend);

    g_rw_lock_writer_unlock(&bs->backends_lock);

    return;
}

/*
 * FIXME: 1) remove _set_address, make this function callable with result of same
 *        2) differentiate between reasons for "we didn't add" (now -1 in all cases)
 */
int network_backends_add(network_backends_t *bs, /* const */ gchar *address, backend_type_t type) {
    network_backend_t *new_backend;
    guint i;

    new_backend = network_backend_new(bs->event_thread_count);
    new_backend->type = type;

    gchar *pos_tag = NULL;
    gchar *pos_weight = NULL;
    if (type == BACKEND_TYPE_RO) {
        guint weight = 1;
        pos_tag = strrchr(address, '$');
        if (pos_tag != NULL) {
            *pos_tag = '\0';
            pos_weight = strrchr(pos_tag + 1, '@');

            new_backend->slave_tag = g_string_new(NULL);
            if (pos_weight != NULL) {
                *pos_weight = '\0';
                weight = strtol(pos_weight+1, NULL, 10);
                g_string_append_len(new_backend->slave_tag, pos_tag+1, pos_weight-pos_tag-1);
            } else {
                g_string_append(new_backend->slave_tag, pos_tag+1);
            }
        } else {
            pos_weight = strrchr(address, '@');
            if (pos_weight != NULL) {
                *pos_weight = '\0';
                weight = strtol(pos_weight+1, NULL, 10);
            }
        }
        new_backend->weight = weight;
    }

    if (0 != network_address_set_address(new_backend->addr, address)) {
        network_backend_free(new_backend);
        g_log_dbproxy(g_critical, "add backend %s failed", address);
        return -1;
    }

    /* check if this backend is already known */
    g_rw_lock_writer_lock(&bs->backends_lock);  /*remove lock*/
    gint first_slave = -1;
    for (i = 0; i < bs->backends->len; i++) {
        network_backend_t *old_backend = bs->backends->pdata[i];

        if (first_slave == -1 && old_backend->type == BACKEND_TYPE_RO) first_slave = i;

        if (old_backend->type == type && strleq(S(old_backend->addr->name), S(new_backend->addr->name))) {
            network_backend_free(new_backend);

            g_rw_lock_writer_unlock(&bs->backends_lock);    /*remove lock*/
            g_log_dbproxy(g_warning, "backend %s is already known!", address);
            return -1;
        }
    }

    g_ptr_array_add(bs->backends, new_backend);
    if (first_slave != -1 && type == BACKEND_TYPE_RW) {
        network_backend_t *temp_backend = bs->backends->pdata[first_slave];
        bs->backends->pdata[first_slave] = bs->backends->pdata[bs->backends->len - 1];
        bs->backends->pdata[bs->backends->len - 1] = temp_backend;
    }

    if (type == BACKEND_TYPE_RO) {
        if (new_backend->slave_tag != NULL) {
            tag_backends_insert(bs, new_backend->slave_tag->str, new_backend);
            g_log_dbproxy(g_message, "add read-only backend %s to backends with tag:%s",
                                            address, new_backend->slave_tag->str);
        } else {
            g_ptr_array_add(bs->def_backend_tag->backends, new_backend);
            g_wrr_poll_update(bs, bs->def_backend_tag);
            g_log_dbproxy(g_message, "add read-only backend %s to default backends", address);
        }
    }

    g_rw_lock_writer_unlock(&bs->backends_lock);    /*remove lock*/

    if (pos_tag != NULL) *pos_tag = '$';
    if (pos_weight != NULL) *pos_weight = '@';

    g_log_dbproxy(g_message, "add %s backend: %s success", (type == BACKEND_TYPE_RW) ? "read/write" : "read-only", address);

    return 0;
}

network_backend_t *network_backends_get(network_backends_t *bs, guint ndx) {
    if (ndx >= network_backends_count(bs)) return NULL;

    /* FIXME: shouldn't we copy the backend or add ref-counting ? */    
    return bs->backends->pdata[ndx];
}

guint network_backends_count(network_backends_t *bs) {
    guint len;

    len = bs->backends->len;

    return len;
}

static int
strcmp_pp(const void *s1, const void *s2) {
    const gchar *src = *(const gchar **)s1;
    const gchar *dst = *(const gchar **)s2;

    return g_strcmp0(src, dst);
}

static int
ip_match(const void *s1, const void *s2) {
    gchar       *percent_pos = NULL;
    const gchar *client_ip = *(const gchar **)s1;
    const gchar *host_ip = *(const gchar **)s2;
    gsize       cmp_size = 0;
    gint        res = -1;

    if (g_strcmp0(host_ip, "%") == 0) {
        return 0;
    }

    percent_pos = g_strstr_len(host_ip, strlen(host_ip), "%");

    if (percent_pos != NULL) {
        cmp_size = (gsize)(percent_pos - (const gchar *)host_ip);
    } else {
        cmp_size = strlen(host_ip);
    }
    if (cmp_size > 0) {
        res = g_ascii_strncasecmp(client_ip, host_ip, cmp_size);
    }
    g_log_dbproxy(g_debug, "client_ip = %s host_ip = %s", client_ip, host_ip);

    return res;
}

user_info_hval *
user_info_hval_new(GString *hashed_passwd) {
    user_info_hval *hval = g_new0(user_info_hval, 1);

    hval->user_hosts = g_ptr_array_new_with_free_func(g_free);
    hval->backends_tag = g_ptr_array_new_with_free_func(g_free);

    hval->hashed_password = hashed_passwd;
    hval->user_tag_max_weight = 0;

    return hval;
}

static void
user_info_hval_free(user_info_hval *hval) {
    if (hval == NULL) return ;

    if (hval->hashed_password) g_string_free(hval->hashed_password, TRUE);
    if (hval->user_hosts) {
        g_ptr_array_free(hval->user_hosts, TRUE);
    }

    if (hval->backends_tag) {
        g_ptr_array_free(hval->backends_tag, TRUE);
    }

    g_free(hval);
}

GString *
get_hash_passwd(GHashTable *pwd_table, gchar *username, GRWLock *user_mgr_lock) {
    user_info_hval *hval = NULL;
    GString *res = NULL;

    g_assert(username != NULL);
    g_assert(pwd_table != NULL);

    g_rw_lock_reader_lock(user_mgr_lock);
    hval = g_hash_table_lookup(pwd_table, username);
    if (hval != NULL) {
        g_assert(hval->hashed_password->len > 0);

        res = g_string_sized_new(hval->hashed_password->len);
        g_string_assign_len(res, hval->hashed_password->str, hval->hashed_password->len);
        g_rw_lock_reader_unlock(user_mgr_lock);
        return res;
    }
    g_rw_lock_reader_unlock(user_mgr_lock);

    return NULL;
}

network_backends_tag *
get_user_backends(network_backends_t *bs, GHashTable *pwd_table,
                                gchar *username, gchar *backend_tag,
                                GRWLock *user_mgr_lock)
{
    user_info_hval *hval = NULL;
    network_backends_tag    *res = NULL, *tag_backends = NULL;
    guint max_cur_weight = 0;
    guint i;

    g_assert(username != NULL && pwd_table != NULL);

    /*
     * 1st: slave@, force backend
     * 2nd: user backend
     * 3rd: default backend
     *
     *
     * FIXME : lock?
     */
    /* 1st : by force */
    if (backend_tag != NULL) {
        tag_backends = g_hash_table_lookup(bs->tag_backends, backend_tag);
        if (tag_backends != NULL) {
            g_assert(tag_backends->backends->len > 0);

            if (tag_backends->wrr_poll->max_weight == 0) {
                g_wrr_poll_init(tag_backends->wrr_poll, tag_backends->backends);
            }
            goto exit;
        }
    }

    /* 2nd by user */
    g_rw_lock_reader_lock(user_mgr_lock);
    hval = g_hash_table_lookup(pwd_table, username);
    if (hval != NULL && hval->backends_tag->len > 0) {
        hval->user_tag_max_weight = 0;
        for(i = 0; i < hval->backends_tag->len; ++i) {
            gchar* user_backends_tag = (gchar *)g_ptr_array_index(hval->backends_tag, i);

            res = g_hash_table_lookup(bs->tag_backends, user_backends_tag);
            if (res != NULL) {
                if (hval->user_tag_max_weight == 0) {
                    hval->user_tag_max_weight = res->wrr_poll->cur_weight;
                    tag_backends = res;
                } else if (res->wrr_poll->cur_weight > hval->user_tag_max_weight) {
                    hval->user_tag_max_weight = res->wrr_poll->cur_weight;
                    tag_backends = res;
                }
            }
        }
    }
    g_rw_lock_reader_unlock(user_mgr_lock);

    /* 3rd by default */
    if (tag_backends == NULL && bs->def_backend_tag->backends->len > 0) {
        tag_backends = bs->def_backend_tag;
    }

exit:

    return tag_backends;
}

static gint
insert_sorted_array(GPtrArray *str_array, void *target, int (*compar)(const void *, const void *)) {
    gint ret = -1;
    gchar **res = NULL;

    g_assert(target != NULL);
    g_assert(str_array != NULL);

    res = bsearch(&target,
            str_array->pdata,
            str_array->len,
            sizeof(str_array->pdata[0]),
            compar);

    if (res != NULL) {
        ret = -1;
    } else {
        g_ptr_array_add(str_array, target);
        g_ptr_array_sort(str_array, compar);
        ret = 0;
    }
    return ret;
}

static gint
delete_sorted_array(GPtrArray *str_array, const char *target, int (*compar)(const void *, const void *)) {
    gint    ret = -1;
    gint    i = 0;

    g_assert(target != NULL);
    g_assert(str_array != NULL);

    for (i = 0; i < str_array->len; i++) {
        gchar *value = str_array->pdata[i];
        if (compar(&value, &target) == 0) {
            g_ptr_array_remove_index(str_array, i);
            ret = 0;
            break;
        }
    }

    return ret;
}

gboolean
check_user_host(GHashTable *pwd_table, gchar *username, gchar *client_ip, GRWLock *user_mgr_lock) {
    user_info_hval  *hval = NULL;
    gboolean        res = FALSE;

    g_assert(username != NULL && client_ip != NULL && pwd_table != NULL);

    g_rw_lock_reader_lock(user_mgr_lock);
    
    //1 check whitelist first
    hval = g_hash_table_lookup(pwd_table, WHITELIST_USER);
    g_assert(hval != NULL);
    if (hval->user_hosts->len > 0) {
        gchar **result = bsearch(&client_ip,
                            hval->user_hosts->pdata,
                            hval->user_hosts->len,
                            sizeof(hval->user_hosts->pdata[0]),
                            ip_match);
        if (result == NULL) { goto end; }
    }

    //2 check user's host
    hval = g_hash_table_lookup(pwd_table, username);
    if (hval == NULL || hval->user_hosts->len == 0) {
            res = TRUE;
    } else {
        gchar **result = bsearch(&client_ip,
                             hval->user_hosts->pdata,
                             hval->user_hosts->len,
                             sizeof(hval->user_hosts->pdata[0]),
                             ip_match);
        if (result != NULL) { res = TRUE; }
    }

end:
    g_rw_lock_reader_unlock(user_mgr_lock);

    g_log_dbproxy(g_debug, "user %s@%s was %s", username, client_ip, res ? "accepted" : "forbidden");
    return res;
}

static void
concatenate_str_array(GPtrArray *str_array, gchar **result, gchar *sep) {
    GString *all_item = NULL;

    g_assert(str_array != NULL && result != NULL && sep != NULL);

    if (*result) {
        g_free(*result);
        *result = NULL;
    }

    if (str_array->len > 0) {
        gint        i = 0;

        all_item = g_string_new(NULL);
        for (i = 0; i < str_array->len; i++) {
            gchar *item = (gchar *)str_array->pdata[i];
            if (item != NULL) {
                g_string_append_printf(all_item, "%s%s", item,
                                (i == str_array->len - 1) ? "" : sep);
            }
        }
        *result = g_strdup(all_item->str);
        g_string_free(all_item, TRUE);
    }
}

int
user_hosts_handle(network_backends_t *bs, const gchar *user, const gchar *raw_user_hosts, gint type) {
    GHashTable      *user_ht = bs->pwd_table;
    user_info_hval  *hval = NULL;
    gchar           *token = NULL;
    gchar           *user_hosts = NULL;
    gchar           *user_hosts_for_free = NULL;
    gint            ret = -1;

    g_assert(user != NULL);

    g_rw_lock_writer_lock(&bs->user_mgr_lock);
    hval = g_hash_table_lookup(user_ht, user);

    if (hval == NULL) {
        ret = -1;
        goto funcexit;
    }

    if (type == REMOVE_USER_HOST && (raw_user_hosts == NULL || strlen(raw_user_hosts) == 0)) {
        if (hval->user_hosts->len > 0) {
            g_ptr_array_remove_range(hval->user_hosts, 0, hval->user_hosts->len);
        }
        goto funcexit;
    }

    user_hosts_for_free = user_hosts = g_strdup(raw_user_hosts);
    while ((token = strsep(&user_hosts, IPS_SEP)) != NULL) {
        gchar *real_ip = get_real_ip(g_strstrip(token));
        if (real_ip == NULL) continue;
        if (type == ADD_USER_HOST) {
            g_log_dbproxy(g_debug, "to add user host:%s@%s", user, real_ip);
            ret = insert_sorted_array(hval->user_hosts, real_ip, ip_match); // should be ip_match
            if (ret != 0) {
                g_log_dbproxy(g_warning, "user host: %s@%s exists, added failed", user, real_ip);
                g_free(real_ip);
            }
        } else if (type == REMOVE_USER_HOST) {
            g_log_dbproxy(g_debug, "to delete user host:%s@%s", user, real_ip);
            ret = delete_sorted_array(hval->user_hosts, real_ip, strcmp_pp);  // should be ip_match
            if (ret != 0) {
                g_log_dbproxy(g_warning, "user host: %s@%s doesn't exist, removed failed", user, real_ip);
            }
            g_free(real_ip);
        }
    }

funcexit:

    if (user_hosts_for_free) g_free(user_hosts_for_free);

    if (hval != NULL) {
        gint i = 0;
        for (i = 0; i < bs->raw_pwds->len; ++i) {
            raw_user_info *rwi = g_ptr_array_index(bs->raw_pwds, i);
            if (g_strcmp0(rwi->username, user) == 0) {
                concatenate_str_array(hval->user_hosts, &rwi->user_hosts, IPS_SEP);
                break;
            }
        }
    }

    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    return ret;
}

int
user_backends_handle(network_backends_t *bs, const gchar *user,
                                    const gchar *user_backends, gint type)
{
    GHashTable      *user_ht = bs->pwd_table;
    user_info_hval  *hval = NULL;
    gchar           *token = NULL;
    gchar           *sep_backends = NULL;
    gchar           *user_backends_for_free = NULL;
    gint            ret = -1;

    g_assert(user != NULL);

    g_rw_lock_writer_lock(&bs->user_mgr_lock);

    hval = g_hash_table_lookup(user_ht, user);
    if (hval == NULL) {
        ret = -1;
        goto funcexit;
    }

    if (type == REMOVE_BACKENDS &&
                    (user_backends == NULL || strlen(user_backends) == 0)) {
        g_ptr_array_remove_range(hval->backends_tag, 0, hval->backends_tag->len);
        hval->user_tag_max_weight = 0;
        g_log_dbproxy(g_message, "to delete all user backends:%s", user);
        goto funcexit;
    }

    user_backends_for_free = sep_backends = g_strdup(user_backends);
    while ((token = strsep(&sep_backends, BACKENDS_SEP)) != NULL) {
        gchar *new_token = NULL;

        if (type == ADD_BACKENDS) {
            if(0 == strlen(token)) continue; //NULL will cause segfault
    
            g_log_dbproxy(g_message, "to add user backend:%s@%s", user, token);

            new_token = g_strdup(token);
            ret = insert_sorted_array(hval->backends_tag, new_token, strcmp_pp);
            if (ret != 0) {
                g_log_dbproxy(g_warning, "user backend: %s@%s exists, added failed", user, token);
                g_free(new_token);
            }
        } else if (type == REMOVE_BACKENDS) {
            if(0 == strlen(token)) continue;

            g_log_dbproxy(g_message, "to delete user backend:%s@%s", user, token);
            ret = delete_sorted_array(hval->backends_tag, token, strcmp_pp);
            hval->user_tag_max_weight = 0;
            if (ret != 0) {
                g_log_dbproxy(g_warning, "user backend: %s@%s doesn't exist, removed failed", user, token);
            }
        }
    }

funcexit:

    if (user_backends_for_free) g_free(user_backends_for_free);

    if (hval != NULL) {
        gint i = 0;
        for (i = 0; i < bs->raw_pwds->len; ++i) {
            raw_user_info *rwi = g_ptr_array_index(bs->raw_pwds, i);
            if (g_strcmp0(rwi->username, user) == 0) {
                concatenate_str_array(hval->backends_tag, &rwi->backends, BACKENDS_SEP);
                hval->user_tag_max_weight = 0;
                g_log_dbproxy(g_debug, "user %s backend: %s", user, rwi->backends);
                break;
            }
        }
    }

    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    return ret;
}

gint
alter_slave_weight(network_backends_t *bs, gint idx, gint weight)
{
    network_backend_t       *backend = NULL;
    network_backends_tag    *tag_backends = NULL;
    gint ret = -1;

    if (0 >= weight) {
        g_log_dbproxy(g_critical, "current weight should > 0, now is %d ", weight);
        return ret;
    }

    g_rw_lock_writer_lock(&bs->backends_lock);

    backend = network_backends_get(bs,idx);
    if(NULL == backend) {
        g_log_dbproxy(g_critical, "current backend_ndx %d is not exist ", idx);
        goto exit;
    }

    if(BACKEND_TYPE_RO == backend->type) {
        backend->weight = weight;
        if(NULL == backend->slave_tag) {
            tag_backends = bs->def_backend_tag;
        } else {
            tag_backends = g_hash_table_lookup(bs->tag_backends,
                                                  backend->slave_tag->str);
            g_assert(tag_backends != NULL);
        }

        g_wrr_poll_update(bs, tag_backends);
        ret = 0;
    } else {
        g_log_dbproxy(g_warning, "backend_ndx %d is BACKEND_TYPE_RW, set weight failed", idx);
    }

exit:
    g_rw_lock_writer_unlock(&bs->backends_lock);
    return ret;
}

gint
add_slave_tag(network_backends_t *bs, gchar *tagname, gchar *idxs)
{
    network_backend_t *backend = NULL;
    network_backends_tag *tag_backends = NULL, *tag_backends_local = NULL;
    gchar *indexs = NULL, *idx_free = NULL;
    gchar *token = NULL;
    gchar                   *key = NULL;
    gint        idx = 0;
    gint ret = 1;

    if(NULL == idxs ||0 >= strlen(idxs)) {
        g_log_dbproxy(g_critical, "current backend_ndx is invalid");
        return ret;
    }

    g_assert(tagname != NULL);

    g_rw_lock_writer_lock(&bs->backends_lock);

    if (g_hash_table_lookup_extended(bs->tag_backends, tagname,
                        (gpointer*)(&key), (gpointer*)(&tag_backends))) {
        g_hash_table_steal(bs->tag_backends, tagname);

        g_assert(NULL != key);
        g_free(key);
    } else {
        tag_backends = tag_backends_new();
    }

    g_assert(NULL != tag_backends);

    idx_free = indexs = g_strdup(idxs);
    while(NULL != (token = strsep(&indexs,COMMA_SEP))) {
        idx = atoi(token) - 1;
        backend = network_backends_get(bs,idx);

        if(NULL != backend && BACKEND_TYPE_RO == backend->type) {
            if (NULL != backend->slave_tag &&
                    0 == strcmp(backend->slave_tag->str, tagname)) {
                continue;
            }

            if(NULL != backend->slave_tag && 0 < backend->slave_tag->len) {
                tag_backends_local = g_hash_table_lookup(bs->tag_backends,
                                                        backend->slave_tag->str);
                if(NULL != tag_backends_local) {
                    g_ptr_array_remove(tag_backends_local->backends, backend);
                    if (0 == tag_backends_local->backends->len) {
                        remove_user_tag_backend(bs, backend->slave_tag->str);
                        g_hash_table_remove(bs->tag_backends,backend->slave_tag->str);
                    }
                }
            }

            g_string_free(backend->slave_tag,TRUE);
            backend->slave_tag = g_string_new(tagname);
            g_ptr_array_add(tag_backends->backends, backend);
            g_ptr_array_remove(bs->def_backend_tag->backends, backend);
        }
    }

    if(0 < tag_backends->backends->len) {
        g_hash_table_insert(bs->tag_backends, g_strdup(tagname), tag_backends);
    } else {
        tag_backends_free(tag_backends);
    }
    g_rw_lock_writer_unlock(&bs->backends_lock);

    g_free(idx_free);
    return ret;
}

gint
remove_slave_tag(network_backends_t *bs, gchar *tagname, gchar *idxs)
{
    network_backend_t *backend = NULL;
    network_backends_tag *tag_backends = NULL;
    GString                 *tag_string = NULL;
    gint                    ret = 0;

    g_assert(tagname != NULL);

    tag_string = g_string_new(tagname);

    g_rw_lock_writer_lock(&bs->backends_lock);
    if (NULL == idxs ||0 == strlen(idxs)) {
        gint loop = 0, count = 0;

        tag_backends = g_hash_table_lookup(bs->tag_backends, tagname);
        if(NULL == tag_backends) {
            g_log_dbproxy(g_warning, "current tag %s is not exist", tagname);
            ret = -1;
            goto exit;
        }

        count = tag_backends->backends->len;
        for(loop = 0; loop<count; loop++) {
            backend = tag_backends->backends->pdata[loop];
            delete_backend_tagname(bs, tag_backends, backend, tag_string);
            loop-=(count - tag_backends->backends->len);
            count = tag_backends->backends->len;
        }

        remove_user_tag_backend(bs, tagname);
        g_hash_table_remove(bs->tag_backends,tagname);
    } else {
        gchar *token = NULL;
        gchar   *indexs = NULL, *idx_free = NULL;
        gint    idx = 0;

        tag_backends = g_hash_table_lookup(bs->tag_backends, tagname);
        if(NULL == tag_backends) {
            g_log_dbproxy(g_warning, "tag %s is not exist", tagname);
            ret = -1;
            goto exit;
        }

        idx_free = indexs = g_strdup(idxs);
        while(NULL != (token = strsep(&indexs, COMMA_SEP))) {
            idx = atoi(token)-1;
            backend = network_backends_get(bs,idx);
            delete_backend_tagname(bs, tag_backends, backend, tag_string);
        }

        if (tag_backends->backends->len == 0) {
            remove_user_tag_backend(bs, tagname);
            g_hash_table_remove(bs->tag_backends,tagname);
        }

        if (idx_free) g_free(idx_free);
    }
exit:
    g_rw_lock_writer_unlock(&bs->backends_lock);
    if (tag_string != NULL) g_string_free(tag_string, TRUE);

    return ret;
}

admin_user_info *
admin_user_info_new()
{
    admin_user_info *au = g_new0(admin_user_info, 1);
    g_rw_lock_init(&au->admin_user_lock);
    au->admin_hosts = g_ptr_array_new_with_free_func(g_free);
    au->name = NULL;
    au->password = NULL;

    return au;
}

void
admin_user_info_free(admin_user_info *au)
{
    if (au == NULL) { return ;}

    g_ptr_array_free(au->admin_hosts, TRUE);
    g_rw_lock_clear(&au->admin_user_lock);

    if (au->name) { g_free(au->name); }
    if (au->password) { g_free(au->password); }

    g_free(au);

    return ;
}
gint
admin_user_host_add(admin_user_info *au, const gchar *host)
{
    gint ret = 0;
    gchar *host_str = g_strdup(host);

    g_assert(au != NULL && host != NULL);

    ret = insert_sorted_array(au->admin_hosts, host_str, ip_match);
    if (ret != 0) {
        g_free(host_str);
    }

    return ret;
}
gint
admin_user_host_remove(admin_user_info *au, const gchar *host)
{
    gint    ret = 0;

    g_assert(au != NULL && host != NULL);
    ret = delete_sorted_array(au->admin_hosts, host, strcmp_pp);
    return ret;
}

gint
admin_user_host_check(admin_user_info *au, const gchar *client_ip)
{
    gint ret = 0;

    g_assert(au != NULL && client_ip != NULL);

    if (au->admin_hosts->len > 0) {
        gchar **result = bsearch(&client_ip,
                     au->admin_hosts->pdata,
                     au->admin_hosts->len,
                     sizeof(au->admin_hosts->pdata[0]),
                     ip_match);
        if (result == NULL) { ret = 1; }
    }

    return ret;
}

GString *
admin_user_hosts_show(admin_user_info *au)
{
    GString *hosts = NULL;
    gint i = 0;

    if (au->admin_hosts->len > 0) {
        hosts = g_string_new(NULL);
        for (i = 0; i < au->admin_hosts->len; i++) {
            gchar *host = (gchar *)au->admin_hosts->pdata[i];
            g_string_append_printf(hosts, "%s%s", host,
                                   (i == au->admin_hosts->len - 1) ? "" : ",");
        }
    }

    return hosts;
}
gint
admin_user_user_update(admin_user_info *au, const gchar *user, const gchar *pwd)
{
    g_assert(au != NULL && user != NULL && pwd != NULL);

    g_rw_lock_writer_lock(&au->admin_user_lock);
    if (au->name != NULL) {
        g_free(au->name);
    }
    if (au->password) {
        g_free(au->password);
    }
    au->name = g_strdup(user);
    au->password = g_strdup(pwd);
    g_rw_lock_writer_unlock(&au->admin_user_lock);

    return 0;
}

/* return value should be free outside */
gchar *
get_real_ip(gchar *token) {
    gchar *real_ip = NULL;

    g_assert(NULL != token);

    if (strlen(token) <= 15) {//xxx.xxx.xxx.xxx 15
        gchar *percent_pos = g_strstr_len(token, strlen(token), IP_END);
        if (percent_pos != NULL) {
            real_ip = g_strndup(token, percent_pos - token + 1);
        } else {
            real_ip = g_strndup(token, strlen(token));
        }
    }
    return real_ip;
}

static gint
remove_user_tag_backend(network_backends_t *bs, gchar *tagname)
{
    raw_user_info *user_info = NULL;
    user_info_hval *user_hval = NULL;
    int loop = 0;

    g_rw_lock_writer_lock(&bs->user_mgr_lock);
    for(loop = 0; loop < bs->raw_pwds->len; loop++) {
        user_info = bs->raw_pwds->pdata[loop];
        user_hval = g_hash_table_lookup(bs->pwd_table, user_info->username);
        delete_sorted_array(user_hval->backends_tag, tagname, strcmp_pp);
        concatenate_str_array(user_hval->backends_tag, &user_info->backends, BACKENDS_SEP);
        user_hval->user_tag_max_weight = 0;
    }
    g_rw_lock_writer_unlock(&bs->user_mgr_lock);

    return 0;
}

static gint
delete_backend_tagname(network_backends_t *bs, network_backends_tag *tag_backends,
                                    network_backend_t *backend, GString *tag_string)
{
    if(NULL != backend && BACKEND_TYPE_RO == backend->type) {
        if (NULL != backend->slave_tag &&
                    g_string_equal(backend->slave_tag, tag_string)) {
            g_string_free(backend->slave_tag, TRUE);
            backend->slave_tag = NULL;
            g_ptr_array_remove(tag_backends->backends, backend);
            g_ptr_array_add(bs->def_backend_tag->backends, backend);
        }
    }

    return 0;
}

static gint
g_wrr_poll_update(network_backends_t *bs, network_backends_tag *tag_backends)
{
    g_assert(tag_backends != NULL);

            tag_backends->wrr_poll->max_weight = 0;
            g_wrr_poll_init(tag_backends->wrr_poll, tag_backends->backends);

    return 0;
}
