#ifndef _PROXY_PERCENTILE_H_
#define _PROXY_PERCENTILE_H_

#include <glib.h>


#define PROXY_PERCENTILE_THREAD        "check_percentile"

#define MAX_RESPONSE_TIME_HIST_LEVELS_PT    23
#define MAX_DATA_NUM_HOUR                   23
#define MAX_DATA_NUM_MIN                    59
#define MAX_DATA_NUM_SEC                    1
#define SECONDS_PER_MIN                     60


typedef enum pt_percentile_state {
    pt_off = 0,
    pt_on 
} pt_percentile_state;

typedef enum pt_time_type {
    PERCENTILE_MIN = 0,
    PERCENTILE_HOUR = 1,
    PERCENTILE_TYPE_ERROR = 2
} pt_time_type;

typedef struct pt_histc_t {
    guint64 response_num[MAX_RESPONSE_TIME_HIST_LEVELS_PT];
    guint   number;
} pt_histc_t;

typedef struct pt_queue_t {
    pt_histc_t *base;
    gint   front;
    gint   rear;
    gint   max_size;
} pt_queue_t;

typedef struct pt_percentile_t {
    pt_queue_t sec_statis;
    pt_queue_t min_statis;
    pt_queue_t hor_statis;
    gdouble range_min;
    gdouble range_max;
    gdouble range_deduct;
    gdouble range_mult;
    gint need_update_num;
    gint percentile_switch;
    gint percentile_value;
} pt_percentile_t;

void* check_percentile(void *user_data);
pt_percentile_t *pt_percentile_new(gdouble range_min,
                                    gdouble range_max,
                                    gint base);
void pt_percentile_free(pt_percentile_t *percentile_controller);
void pt_percentile_update(gdouble value, pt_percentile_t *percentile_controller);
gdouble pt_percentile_calculate(guint latest_time,
                                    pt_time_type time_type,
                                    pt_percentile_t *percentile_controller);
void pt_percentile_reset(pt_percentile_t *percentile_controller);
gint pt_get_response_time(const gchar *value,
                          pt_percentile_t *percentile_controller,
                          gdouble *result);

gint assign_percentile_switch(const char *newval, void *ex_param);
gint assign_percentile_value(const char *newval, void *ex_param);
gchar *show_percentile(void *ex_param);
gchar *show_percentile_switch(void *external_param);
gchar *show_percentile_value(void *external_param);

/* param mgr */
#endif
