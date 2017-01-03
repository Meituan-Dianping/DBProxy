#ifndef _CHASSIS_FILTER_H_
#define _CHASSIS_FILTER_H_

#include <glib.h>

/* Filter */
#define RQ_NO_STATUS            0x00
#define RQ_FOBIDDEN_BY_FILTER   0x01
#define RQ_HIT_BY_FILTER        0x02
#define AUTO_ADD_FILTER         0x10
#define MANUAL_ADD_FILTER       0x20

/* Reserved query*/
#define RQ_OVER_TIME            0x01
#define RQ_OVER_FREQ            0x02
#define RQ_PRIORITY_0           ~RQ_OVER_TIME & RQ_OVER_FREQ
#define RQ_PRIORITY_1           RQ_OVER_TIME & ~RQ_OVER_FREQ
#define RQ_PRIORITY_2           RQ_OVER_TIME & RQ_OVER_FREQ
#define RQ_PRIORITY_3           ~RQ_OVER_TIME & ~RQ_OVER_FREQ


#define MICROSEC (1000*1000)
#define FREQ_INTERVAL   60


/* filter */
typedef struct sql_filter_hval
{
    int flag;
    int filter_status;
    volatile int hit_times;
    gchar *sql_filter_item;
} sql_filter_hval;

typedef struct sql_filter
{
    GHashTable  *blacklist;
    gint        auto_filter_flag;
    gint        manual_filter_flag;
    gchar       *blacklist_file;
    GRWLock     sql_filter_lock;
} sql_filter;

extern sql_filter *sql_filter_new(int flag);
extern void sql_filter_free(sql_filter *filter);
extern gboolean sql_filter_insert(sql_filter *filter, const gchar *sql_rewrite,
                                    gchar *sql_md5str, int flag, int filter_status);
extern sql_filter_hval *sql_filter_lookup(sql_filter *filter, const gchar *sql_md5str);
extern gboolean sql_filter_remove(sql_filter *filter, const gchar *sql_md5str);
extern void sql_filter_show(sql_filter *filter);

extern GString *sql_filter_sql_rewrite(GPtrArray *tokens);

extern int load_sql_filter_from_file(sql_filter *cur_filter);

/* sql reserved query */
typedef struct reserved_query_item
{
     time_t   item_first_access_time;
     time_t   item_last_access_time;
     time_t   item_gap_start_time;
     gint     item_status;
     gint     item_access_num;
     gint     item_gap_access_num;
     GString  *item_rewrite;
     GString  *item_rewrite_md5;
     GList    *list_pos;
     GMutex   rq_item_lock;
} reserved_query_item;

typedef struct sql_reserved_query
{
    GHashTable  *ht_reserved_query;
    GQueue      *gq_reserved_short_query;
    GQueue      *gq_reserved_long_query;
    volatile gint  lastest_query_num;
    volatile gint  freq_time_window;
    volatile gint  access_num_per_time_window;
    volatile gint  query_filter_time_threshold;
    gdouble query_filter_frequent_threshold;
    GRWLock     rq_lock;
} sql_reserved_query;

extern sql_reserved_query *sql_reserved_query_new(void);
extern void sql_reserved_query_free(sql_reserved_query *srq);
extern void sql_reserved_query_insert(sql_reserved_query *srq, reserved_query_item *rq);
extern void sql_reserved_query_rebuild(sql_reserved_query *srq, gint max_query_num);
extern reserved_query_item *sql_reserved_query_lookup(sql_reserved_query *srq, const char *sql_md5);
extern reserved_query_item *reserved_query_item_new(gchar *sql_rewrite, gchar *sql_rewrite_hash);
extern void reserved_query_item_free(reserved_query_item *rqi);

extern gint rq_item_compare(reserved_query_item *a, gchar *b);
extern void sql_reserved_query_move_to_tail(sql_reserved_query *srq, reserved_query_item *rqi);
extern void set_freq_time_windows(sql_reserved_query *rq, gdouble freq, gint gap_threadhold);

#endif /* _CHASSIS_FILTER_H_ */
