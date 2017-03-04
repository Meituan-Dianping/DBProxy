#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "lib/sql-tokenizer.h"
#include "chassis-filter.h"
#include "chassis-frontend.h"
#include "chassis-log.h"

#define C_S(x) x, strlen(x)

/* filter */
static sql_filter_hval *sql_filter_hval_new(const gchar *sql_rewrite, int flag, int filter_status);
static void sql_filter_hval_free(sql_filter_hval* hval);
static void sql_filter_item_show(gpointer key, gpointer value, gpointer userdata);

static gboolean sql_filter_equal_func(gchar *sql1, gchar *sql2);
/* filter end */

/* reserved query */
static void g_queue_travel(GQueue *reserved_query);
/* reserved query end */

/* Definition of filter */
sql_filter *
sql_filter_new(int flag)
{
    sql_filter *filter = g_new0(sql_filter, 1);
    filter->blacklist = g_hash_table_new_full((GHashFunc)g_str_hash,
                                    (GEqualFunc)sql_filter_equal_func,
                                    g_free,
                                    (GDestroyNotify) sql_filter_hval_free);
    filter->auto_filter_flag = 0;
    filter->manual_filter_flag = 0;
    g_rw_lock_init(&filter->sql_filter_lock);

    return filter;
}

void
sql_filter_free(sql_filter *filter)
{
    if (filter == NULL) return ;

    g_hash_table_remove_all(filter->blacklist);
    g_hash_table_destroy(filter->blacklist);
    g_rw_lock_clear(&filter->sql_filter_lock);
    g_free(filter->blacklist_file);
    g_free(filter);
    return ;
}

gboolean
sql_filter_insert(sql_filter *filter, const gchar *sql_rewrite, gchar *sql_md5str, int flag, int filter_status)
{
    if (filter == NULL) return FALSE;   

    sql_filter_hval *hval = sql_filter_hval_new(sql_rewrite, flag, filter_status);

    return g_hash_table_insert(filter->blacklist, g_strdup(sql_md5str), hval);
}

sql_filter_hval *
sql_filter_lookup(sql_filter *filter, const gchar *sql_md5str)
{
    if (filter == NULL || (filter != NULL && filter->blacklist == NULL)) return NULL;
    if (sql_md5str == NULL) return NULL;    

    return g_hash_table_lookup(filter->blacklist, sql_md5str);
}

gboolean
sql_filter_remove(sql_filter *filter, const gchar *sql_md5str)
{
    if (filter == NULL || (filter != NULL && filter->blacklist == NULL)) return FALSE;
    if (sql_md5str == NULL) return FALSE;

    return g_hash_table_remove(filter->blacklist, sql_md5str);
}

void
sql_filter_show(sql_filter *filter)
{
    if (filter == NULL) return ;

    g_hash_table_foreach(filter->blacklist, sql_filter_item_show, NULL);
    return ;
}

void
sql_filter_item_show(gpointer key, gpointer value, gpointer userdata)
{
    gchar *hkey = (gchar *) key;
    sql_filter_hval *hval = (sql_filter_hval *) value;

    g_log_dbproxy(g_message, "key = %s\tkval.flag = %d\tkval.hit_times = %dkval.sql_filter_item = %s",
                        hkey, hval->flag, hval->hit_times, hval->sql_filter_item);
}

static gboolean
sql_filter_equal_func(gchar *sql1, gchar *sql2)
{
    return (strcasecmp(sql1, sql2) == 0);
}

static sql_filter_hval *
sql_filter_hval_new(const gchar *sql_rewrite, int flag, int filter_status)
{
    sql_filter_hval *hval = g_new0(sql_filter_hval, 1);

    hval->sql_filter_item = g_strdup(sql_rewrite);
    hval->flag = flag;
    hval->filter_status = filter_status;
    hval->hit_times = 0;

    return hval;
}

GString *
sql_filter_sql_rewrite(GPtrArray *tokens)
{
    GString *sql_rewrite = NULL;
    int i = 0;

    if (tokens == NULL) return NULL;

    sql_rewrite = g_string_new(NULL);

    for (i = 0; i < tokens->len; i++)
    {
        sql_token *tk = (sql_token *)tokens->pdata[i];
        switch (tk->token_id)
        {
            case TK_UNKNOWN:
            case TK_COMMENT:
                break;
            case TK_SEMICOLON:
            {
                if (i == tokens->len - 1) { g_string_truncate(sql_rewrite, sql_rewrite->len - 1); }
                break;
            }
            case TK_INTEGER:
            case TK_FLOAT:
            case TK_STRING:
            {
                g_string_append_printf(sql_rewrite, "?%s", (i == tokens->len - 1) ? "" : " ");
                break;
            }
            case TK_LITERAL:
            {
                /* The ID like table name must keep the original type. */
                g_string_append_printf(sql_rewrite, "%s%s", tk->text->str,
                                            (i == tokens->len - 1) ? "" : " ");
                break;
            }
            default:
            {
                GString *lower = g_string_ascii_down(g_string_new(tk->text->str));
                g_string_append_printf(sql_rewrite, "%s%s", lower->str,
                                    (i == tokens->len - 1) ? "" : " ");
                g_string_free(lower, TRUE);
            }
        }
    }

    return sql_rewrite;
}

static void
sql_filter_hval_free(sql_filter_hval* hval)
{
    if (hval == NULL) return ;

    g_free(hval->sql_filter_item);
    g_free(hval);

    return ;
}

int
load_sql_filter_from_file(sql_filter *cur_filter)
{
    GKeyFile *blacklist_config = NULL;
    GError *gerr = NULL;
    gsize length = 0;
    int i = 0;
    gchar **groups = NULL;

    g_assert(cur_filter != NULL);

    g_rw_lock_reader_lock(&cur_filter->sql_filter_lock);
    if (!(blacklist_config = chassis_frontend_open_config_file(cur_filter->blacklist_file, &gerr)))
    {
        g_log_dbproxy(g_warning, "[filter][load from file][failed][%s]", gerr->message);
        g_error_free(gerr);
        gerr = NULL;
        g_rw_lock_reader_unlock(&cur_filter->sql_filter_lock);
        return 1;
    }
    g_rw_lock_reader_unlock(&cur_filter->sql_filter_lock);

    groups = g_key_file_get_groups(blacklist_config, &length);
    for (i = 0; groups[i] != NULL; i++)
    {
        gchar *sql_rewrite_md5 = NULL;
        int is_enabled = 0;
        int filter_status = 0;

        gchar *filter = g_key_file_get_value(blacklist_config, groups[i], "filter", &gerr);
        if (gerr != NULL) { goto next; }

        is_enabled = g_key_file_get_integer(blacklist_config, groups[i], "is_enabled", &gerr);
        if (gerr != NULL) { goto next; }

        filter_status = g_key_file_get_integer(blacklist_config, groups[i], "filter_status", &gerr);
        if (gerr != NULL) { goto next; }

        sql_rewrite_md5 = g_compute_checksum_for_string(G_CHECKSUM_MD5, C_S(filter));
        g_rw_lock_writer_lock(&cur_filter->sql_filter_lock);
        sql_filter_hval *hval = sql_filter_lookup(cur_filter, sql_rewrite_md5);
        if (hval != NULL)
        {
            hval->flag = is_enabled;
            hval->filter_status = filter_status;
        }
        else
        {
            sql_filter_insert(cur_filter, filter, sql_rewrite_md5, is_enabled, filter_status);
        }
        g_rw_lock_writer_unlock(&cur_filter->sql_filter_lock);

next:
        if (filter !=NULL) { g_free(filter); }
        if (sql_rewrite_md5 != NULL) { g_free(sql_rewrite_md5); }

        if (gerr != NULL)
        {
            g_log_dbproxy(g_warning, "[filter][load from file][failed][%s]", gerr->message);
            g_error_free(gerr);
            gerr = NULL;
        }
        else
            g_log_dbproxy(g_message, "[filter][load from file %s][success]", cur_filter->blacklist_file);
    }
    g_strfreev(groups);

    g_key_file_free(blacklist_config);
    return 0;
}
/* End of definition of filter */


/* Definition of reserved_query */
sql_reserved_query *
sql_reserved_query_new(void)
{
    sql_reserved_query *srq = g_new0(sql_reserved_query, 1);

    srq->ht_reserved_query = g_hash_table_new_full((GHashFunc)g_str_hash,
                                    (GEqualFunc)sql_filter_equal_func,
                                    g_free,
                                    (GDestroyNotify) reserved_query_item_free);

    srq->gq_reserved_long_query =  g_queue_new();
    srq->gq_reserved_short_query = g_queue_new();

    srq->freq_time_window = 0;

    g_rw_lock_init(&srq->rq_lock);

    return srq;
}

void
sql_reserved_query_free(sql_reserved_query *srq)
{
    if (srq == NULL) return ;

    g_hash_table_remove_all(srq->ht_reserved_query);
    g_hash_table_destroy(srq->ht_reserved_query);
    g_queue_free(srq->gq_reserved_long_query);
    g_queue_free(srq->gq_reserved_short_query);

    g_rw_lock_clear(&srq->rq_lock);

    g_free(srq);
}

reserved_query_item *
sql_reserved_query_lookup(sql_reserved_query *srq, const char *sql_md5)
{
    if (srq == NULL || sql_md5 == NULL) return NULL;
    return g_hash_table_lookup(srq->ht_reserved_query, sql_md5);
}

reserved_query_item *
reserved_query_item_new(gchar *sql_rewrite, gchar *sql_rewrite_hash)
{
    reserved_query_item *rqi = g_new0(reserved_query_item, 1);

    rqi->item_last_access_time = rqi->item_first_access_time = time(NULL);
    rqi->item_gap_start_time = rqi->item_last_access_time;

    rqi->item_rewrite = g_string_new(sql_rewrite);
    rqi->item_rewrite_md5 = g_string_new(sql_rewrite_hash);
    rqi->item_access_num = 1;
    rqi->item_gap_access_num = 0;
    rqi->item_status = RQ_NO_STATUS;

    rqi->list_pos = NULL;

    g_mutex_init(&rqi->rq_item_lock);

    return rqi;
}

void
reserved_query_item_free(reserved_query_item *rqi)
{
    g_assert(rqi != NULL);

    g_mutex_clear(&rqi->rq_item_lock);
    if (rqi->item_rewrite) g_string_free(rqi->item_rewrite, TRUE);
    if (rqi->item_rewrite_md5) g_string_free(rqi->item_rewrite_md5, TRUE);
    g_free(rqi);
}

gint
rq_item_compare(reserved_query_item *a, gchar *b)
{
    return g_ascii_strcasecmp(a->item_rewrite_md5->str, b);
}

void
sql_reserved_query_insert(sql_reserved_query *srq, reserved_query_item *rq)
{
    GQueue *gq = NULL;

    if (srq == NULL || rq == NULL) return ;

    gq = (rq->item_status & RQ_OVER_TIME) ? srq->gq_reserved_long_query : srq->gq_reserved_short_query;

    g_queue_push_tail(gq, rq);
    rq->list_pos = g_queue_peek_tail_link(gq);
}

void
sql_reserved_query_rebuild(sql_reserved_query *srq, gint max_query_num)
{
    GQueue *gq = NULL;
    GList *travel_list = NULL;
    gint i = 0, remove_item_num = 0, time_interval = 0;
    reserved_query_item *rm_rqi = NULL;
    time_t cur_time  = time(NULL);

    g_assert(max_query_num >= 0);

    g_rw_lock_writer_lock(&srq->rq_lock);

    if (srq == NULL) goto exit;
    if (srq->ht_reserved_query == NULL) goto exit ;

    remove_item_num = g_queue_get_length(srq->gq_reserved_short_query);
    remove_item_num += g_queue_get_length(srq->gq_reserved_long_query);
    remove_item_num -= max_query_num;

    if (remove_item_num <= 0) goto exit;

    /* 4 roll lru */
    for(i = 0; i < 4; i++)
    {
        gq = (i%2 == 0) ? srq->gq_reserved_short_query : srq->gq_reserved_long_query;

        if (g_queue_get_length(gq) == 0) continue;

        travel_list = g_queue_peek_head_link(gq);
        while (travel_list != NULL)
        {
            rm_rqi = (reserved_query_item *)travel_list->data;
            g_assert(rm_rqi->list_pos == travel_list);

            if (i < 2 && max_query_num != 0)
            {
               time_interval = cur_time - rm_rqi->item_last_access_time;
               if (time_interval < g_atomic_int_get(&srq->freq_time_window))
               {
                   break;
               }
            }
#ifdef FILTER_DEBUG
            g_log_dbproxy(g_debug, "[reserved query][lru remove][%s]", rm_rqi->item_rewrite->str);
#endif
            travel_list =  g_queue_pop_head_link(gq);
            g_hash_table_remove(srq->ht_reserved_query, rm_rqi->item_rewrite_md5->str);
            g_list_free(travel_list);
            travel_list = NULL;

            if (remove_item_num-- == 0) goto exit;

            travel_list = g_queue_peek_head_link(gq);
        }
    }

exit :

#ifdef FILTER_DEBUG
    g_queue_travel(srq->gq_reserved_long_query);
    g_queue_travel(srq->gq_reserved_short_query);
#endif

    g_rw_lock_writer_unlock(&srq->rq_lock);
    return ;
}

void
sql_reserved_query_move_to_tail(sql_reserved_query *srq, reserved_query_item *rqi)
{
    GList   *to_tail = rqi->list_pos;
    GQueue  *gq = NULL;
    int     ret = 0;

    if (srq == NULL || to_tail == NULL) return;

    ret = g_queue_link_index(srq->gq_reserved_long_query, to_tail);
    gq = (ret != -1) ? (srq->gq_reserved_long_query) : (srq->gq_reserved_short_query);

    if (g_queue_get_length(gq) == 0) return ;

    if (to_tail != gq->tail)
    {
        g_queue_unlink(gq, to_tail);
        g_queue_push_tail_link(gq, to_tail);
        rqi->list_pos = g_queue_peek_tail_link(gq);
    }
#ifdef FILTER_DEBUG
    g_queue_travel(srq->gq_reserved_long_query);
    g_queue_travel(srq->gq_reserved_short_query);
#endif

    return ;
}

void
set_freq_time_windows(sql_reserved_query *rq, gdouble freq, gint gap_threadhold)
{
    int i = ceil((gdouble)gap_threadhold/(freq));
    if (i < FREQ_INTERVAL) {  i = FREQ_INTERVAL; }
    g_atomic_int_set(&rq->freq_time_window, i);
}

static void
g_queue_travel(GQueue *reserved_query)
{
     gint i = 0;
     gint len = 0;

     g_assert(reserved_query != NULL);

     len = reserved_query->length;
     g_log_dbproxy(g_debug, "travel GQueue, %d items", len);
     while (i < len)
     {
         reserved_query_item *rqi = g_queue_peek_nth (reserved_query, i);

         g_log_dbproxy(g_debug, "query = %s, first_time = %lu, last_time = %lu, status = %d",
                             rqi->item_rewrite->str,
                             rqi->item_first_access_time,
                             rqi->item_last_access_time,
                             rqi->item_status);
         i++;
     }
     return ;
}
/* End of Definition of reserved_query */
