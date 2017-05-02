#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <math.h>

#include "chassis-mainloop.h"
#include "chassis-options-utils.h"
#include "proxy-plugin.h"
#include "proxy-percentile.h"


/*
 * unit test declare
 */
#ifdef __MONITOR
pt_percentile_t *percentile;
void data_snap_shot(char* flag);
void show_percentile_data();
void data_inject();
void unit_test();
void histc_snap_shot(char* flag,pt_histc_t histc);
#endif

#ifdef __CONCURRENT
void admin_test_set1();
void admin_test_set2();
void admin_test_set3();
#endif


#define ONE_MINUTE 60
//internal declare
static void pt_queue_reset(pt_queue_t *pq);
static gint pt_queue_new(pt_queue_t *pq, gint max_size);
static void pt_queue_free(pt_queue_t pq);
static gboolean pt_is_queue_empty(pt_queue_t q);
static gboolean pt_is_queue_full(pt_queue_t q);
static void pt_queue_push(pt_queue_t *pq, pt_histc_t base);
static void pt_queue_pop(pt_queue_t q, pt_histc_t *base);
static void pt_queue_push_ex(pt_queue_t *pq, gint no1, gint no2);
static void pt_queue_pop_ex(pt_queue_t q, gint num, pt_histc_t *base);

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

pt_percentile_t *
pt_percentile_new(gdouble range_min, gdouble range_max, gint base)
{
    pt_percentile_t *percentile_controller = NULL;

    percentile_controller = g_new0(pt_percentile_t, 1);

    pt_queue_new(&(percentile_controller->sec_statis), MAX_DATA_NUM_SEC);
    percentile_controller->sec_statis.rear++;
    pt_queue_new(&(percentile_controller->min_statis), MAX_DATA_NUM_MIN);
    pt_queue_new(&(percentile_controller->hor_statis), MAX_DATA_NUM_HOUR);

    percentile_controller->need_update_num = 0;
    percentile_controller->range_max = pow((gdouble)base, range_max);
    percentile_controller->range_min = pow((gdouble)base, range_min);

    percentile_controller->range_deduct = log((gdouble)percentile_controller->range_min);
    percentile_controller->range_mult = (MAX_RESPONSE_TIME_HIST_LEVELS_PT - 1) / (log(percentile_controller->range_max) - percentile_controller->range_deduct);

#ifdef __MONITOR
    percentile = percentile_controller;
    g_thread_try_new("show_percentile_data", (GThreadFunc)show_percentile_data, NULL, NULL);
    //g_thread_try_new("data_injection",(GThreadFunc)data_inject,NULL,NULL);
    //g_thread_try_new("unit_test",(GThreadFunc)unit_test,NULL,NULL);
#endif

#ifdef __CONCURRENT
    g_thread_try_new("admin_test_set1", (GThreadFunc)admin_test_set1, NULL, NULL);
    g_thread_try_new("admin_test_set2", (GThreadFunc)admin_test_set2, NULL, NULL);
    g_thread_try_new("admin_test_read", (GThreadFunc)admin_test_read, NULL, NULL);
#endif

    return percentile_controller;
}

void
pt_percentile_free(pt_percentile_t *percentile_controller)
{
    if (percentile_controller) {
        pt_queue_free(percentile_controller->sec_statis);
        pt_queue_free(percentile_controller->min_statis);
        pt_queue_free(percentile_controller->hor_statis);
        g_free(percentile_controller);
        percentile_controller = NULL;
    }
}

void
pt_percentile_update(gdouble value, pt_percentile_t *percentile_controller)
{
     guint n = 0;
     gdouble val = value;

     if (value < percentile_controller->range_min) {
         val= percentile_controller->range_min;
     } else if (value > percentile_controller->range_max) {
         val= percentile_controller->range_max;
     }

     n = floor((log(val) - percentile_controller->range_deduct) * percentile_controller->range_mult  + 0.5);
     pt_queue_push_ex(&percentile_controller->sec_statis, 0, n);
}

gdouble
pt_percentile_calculate(guint latest_time, pt_time_type time_type,
                                        pt_percentile_t *percentile_controller)
{
    gint loop = 0, nmax = 0, ncur = 0;
     pt_histc_t data = {{0}, 0};

     memset((void*)&data, 0, sizeof(pt_histc_t));

     switch (time_type) {
         case PERCENTILE_MIN: {
            pt_queue_pop_ex(percentile_controller->sec_statis, 1, &data);
            pt_queue_pop_ex(percentile_controller->min_statis, latest_time, &data);
            break ;
         }
         case PERCENTILE_HOUR: {
            pt_queue_pop_ex(percentile_controller->sec_statis, 1, &data);
            pt_queue_pop_ex(percentile_controller->min_statis, percentile_controller->need_update_num, &data);
            pt_queue_pop_ex(percentile_controller->hor_statis, latest_time, &data);
            break ;
         }
         case PERCENTILE_TYPE_ERROR: {
            g_log_dbproxy(g_critical, "unexpected branch encountered: PERCENTILE_TYPE_ERROR");
            break;
         }
     }

#ifdef __MONITOR
      data_snap_shot("calculate");
      histc_snap_shot("calculate", data);
#endif

      nmax = floor((gdouble)(data.number * percentile_controller->percentile_value / 100.0 + 0.5));

      for (loop = 0; loop < MAX_RESPONSE_TIME_HIST_LEVELS_PT; loop ++) {
          ncur += data.response_num[loop];
              if (ncur >=nmax) {
                  break;
              }
      }
      return exp(((gdouble) loop/ percentile_controller->range_mult) + percentile_controller->range_deduct);
 }

void
pt_percentile_reset(pt_percentile_t *percentile_controller)
{
    pt_queue_reset(&percentile_controller->sec_statis);
    percentile_controller->sec_statis.rear++;
    pt_queue_reset(&percentile_controller->min_statis);
    pt_queue_reset(&percentile_controller->hor_statis);

    percentile_controller->need_update_num = 0;
}

gint
pt_get_response_time(const gchar *value, pt_percentile_t *percentile_controller, gdouble *result)
{
    pt_time_type type = PERCENTILE_TYPE_ERROR;
    gint ret = 1;
    gint64 arg = 0;
    gint   idx = 0;
    gchar *ptr = NULL;

    if (pt_off == percentile_controller->percentile_switch) {
        ret = 2;
        goto exit;
    }

    idx = strlen(value) - 1;
    if (value[idx] == 'm') {
        type = PERCENTILE_MIN;
    } else if (value[idx] == 'h') {
        type = PERCENTILE_HOUR;
    } else {
        goto exit;
    } 

    ptr = g_strndup(value, strlen(value) - 1);
    if(try_get_int64_value(ptr, &arg)) {
        if ((PERCENTILE_MIN == type && 0 < arg && 60 > arg ) ||
            (PERCENTILE_HOUR == type && 0 < arg && 24 > arg)) {
            *result = pt_percentile_calculate(arg, type, percentile_controller);        
            ret = 0;
        }
    }

exit:
    if(ptr) g_free(ptr);

    return ret;
}

void pt_queue_reset(pt_queue_t *pq) {
    pq->front = 0;
    pq->rear = 0;
    memset(pq->base, 0, sizeof(pt_histc_t)*(pq->max_size));
}

gint
pt_queue_new(pt_queue_t *pq, gint size)
{
    pq->front = 0;
    pq->rear = 0;
    pq->max_size = size + 1;
    if (NULL != pq->base) {
        g_free(pq->base);
    }
    pq->base = (pt_histc_t*)g_malloc0(sizeof(pt_histc_t)*(pq->max_size));
    return (NULL == pq->base ? -1:0);
}

void
pt_queue_free(pt_queue_t pq)
{
    if (pq.base) {
        g_free(pq.base);
        pq.base = NULL;
    }
}

gboolean
pt_is_queue_empty(pt_queue_t q)
{
    return ((q.front == q.rear) ? 1 : 0);
    }

gboolean
pt_is_queue_full(pt_queue_t q)
{
    return ((q.front == (q.rear + 1) % q.max_size) ? 1 : 0);
}

void
pt_queue_push(pt_queue_t *pq, pt_histc_t base)
{
    if (pt_is_queue_full(*pq)) {
        pq->front = (pq->front + 1)%pq->max_size;
    }
    pq->base[pq->rear] = base;
    pq->rear = (pq->rear + 1)%pq->max_size;
}

void
pt_queue_pop(pt_queue_t q, pt_histc_t *base)
{
    if (pt_is_queue_empty(q)) {
        return;
    }

    gint cur = (q.rear - 1) >= 0 ? (q.rear - 1) : (q.rear - 1 + q.max_size);
    *base = q.base[cur];
}

void pt_queue_push_ex(pt_queue_t *pq, gint no1, gint no2)
{
    pq->base[no1].response_num[no2]++;
    pq->base[no1].number++;
}

void pt_queue_pop_ex(pt_queue_t q, gint num, pt_histc_t *base)
{
    gint i = 0;

    if (pt_is_queue_empty(q)) {
        return;
    }

    if (num > q.max_size-1 || -1 == num) {
        num = q.max_size-1;
    }
    gint cur = (q.rear - 1) >= 0 ? (q.rear - 1) : (q.rear - 1 + q.max_size);
    while (num -- ) {
        for (i=0; i<MAX_RESPONSE_TIME_HIST_LEVELS_PT; i++) {
            base->response_num[i] += q.base[cur].response_num[i];
            base->number += q.base[cur].response_num[i];
        }
        cur = (cur - 1) >= 0 ? (cur - 1) : (cur - 1 + q.max_size);
    }
}

void
pt_queue_pop_mb(pt_queue_t q, gint num, gint pos, gint *b)
{
    if (pt_is_queue_empty(q)) {
        return ;
    }

    if (num > q.max_size-1 || -1 == num) {
        num = q.max_size-1;
    }

    gint cur = (q.rear - 1) >= 0 ? (q.rear - 1) : (q.rear - 1 + q.max_size);
    while (num -- ) {
        b[pos] = q.base[cur].number;
        pos++;
        cur = (cur - 1) >= 0 ? (cur - 1) : (cur - 1 + q.max_size);
    }
}

void *
check_percentile(void *user_data)
{
    plugin_thread_param *plugin_params = (plugin_thread_param *) user_data;
    pt_percentile_t     *percentile_controller = (pt_percentile_t *)plugin_params->magic_value;
    GCond               *g_cond = plugin_params->plugin_thread_cond;
    GMutex              *g_mutex = plugin_params->plugin_thread_mutex;
    pt_histc_t          data = {{0}, 0};
    gint64              end_time = 0;

    g_log_dbproxy(g_message, "%s thread start", PROXY_PERCENTILE_THREAD);
    if (NULL == percentile_controller) {
        g_log_dbproxy(g_critical, "check_percentile thread get argument failed");
        return NULL;
    }

    while (!chassis_is_shutdown()) {
        g_mutex_lock(g_mutex);
        while (percentile_controller->percentile_switch == pt_off) {
            end_time = g_get_monotonic_time () + SECONDS_PER_MIN * G_TIME_SPAN_SECOND;
            if (!g_cond_wait_until(g_cond, g_mutex, end_time)) {
                g_log_dbproxy(g_debug, "percentile waiting meet timeout");
            } else {
                if (chassis_is_shutdown()) {
                    g_log_dbproxy(g_message, "check_percentile thread get exit signal");
                    g_mutex_unlock(g_mutex);
                    goto exit;
                }
            }
        }
        g_mutex_unlock(g_mutex);

#ifdef __MONITOR
    //data_snap_shot("check_thread-begin");
#endif
    memset((void*)&data, 0, sizeof(pt_histc_t));
    data.number = 0;

    if (ONE_MINUTE == percentile_controller->need_update_num + 1) {
        pt_queue_pop_ex(percentile_controller->sec_statis, 1, &data);
        pt_queue_pop_ex(percentile_controller->min_statis, -1, &data);
        pt_queue_push(&percentile_controller->hor_statis, data);
        memset((void*)&data, 0, sizeof(pt_histc_t));
        data.number = 0;
        percentile_controller->need_update_num = 0;
    }
    pt_queue_pop_ex(percentile_controller->sec_statis, 1, &data);
    pt_queue_push(&percentile_controller->min_statis, data);
    percentile_controller->need_update_num++;

    pt_queue_reset(&percentile_controller->sec_statis);
    percentile_controller->sec_statis.rear++;
#ifdef __MONITOR
    //data_snap_shot("check_thread-end");
#endif
    }
exit:
    g_log_dbproxy(g_message, "check_percentile thread will exit");

    g_thread_exit(0);
    return NULL;
}


gchar *
show_percentile_switch(void *ex_param)
{
    gchar *u = NULL;

    if (config->percentile_controller->percentile_switch == pt_off) {
        u = "OFF";
    } else {
        u = "ON";
    }
    return g_strdup(u);
}

gint
assign_percentile_switch(const char *newval, void *ex_param)
{
    gint old_value = config->percentile_controller->percentile_switch;
    gint new_value = 0;
    gint ret = 0;

    if (0 == strcasecmp(newval, "ON")) {
        new_value = pt_on;
    } else if (0 == strcasecmp(newval, "OFF")) {
        new_value = pt_off;
    } else {
        ret = 1;
        goto exit;
    }

    if (new_value != old_value) {
        config->percentile_controller->percentile_switch = new_value;

        if (new_value == pt_off) {
            pt_percentile_reset(config->percentile_controller);
        }

        if (old_value == pt_off) {
            /* wake up the percentil thread */
            plugin_thread_t *percentile_thread = g_hash_table_lookup(config->plugin_threads,
                                                  PROXY_PERCENTILE_THREAD);

            g_mutex_lock(&percentile_thread->thr_mutex);
            g_cond_signal(&percentile_thread->thr_cond);
            g_mutex_unlock(&percentile_thread->thr_mutex);
            g_log_dbproxy(g_message, "wake up %s thread", PROXY_PERCENTILE_THREAD);
        }
    }

exit:
    return ret;
}

gchar *
show_percentile(void *ex_param)
{
    external_param *st = (external_param *)ex_param;
    gdouble percentile = 0.0;
    gint    res = 0;

    res = pt_get_response_time(st->magic_value ? (gchar *)st->magic_value : "1m",
                                config->percentile_controller,
                                &percentile);

    if (res == 0) {
        return g_strdup_printf("%f", percentile);
    } else {
        return g_strdup_printf("%d", res);
    }
}

gchar *
show_percentile_value(void *ex_param)
{
    return g_strdup_printf("%d", config->percentile_controller->percentile_value);
}

gint
assign_percentile_value(const char *newval, void *ex_param)
{
    return set_raw_int_value(newval, &config->percentile_controller->percentile_value, 1, 101);
}

/*
 * the function below is just for unit test
 */
#ifdef __CONCURRENT
void admin_test_set1() {
    gint i = 1;
    while(!chassis_is_shutdown()){
        percentile->percentile_switch = 0;
        percentile->percentile = (((i++)+50)%100+1);
        sleep(1);
    }
}
void admin_test_set2() {
    gint i = 1;
    while (!chassis_is_shutdown()) {
        percentile->percentile_switch = 1;
        percentile->percentile = ((i++)%100+1);
        sleep(1);
    }
}
void admin_test_read() {
    while (!chassis_is_shutdown()) {
        printf("percentile-switch = %d percentile-th = %lf \n",percentile->percentile_switch,percentile->percentile);
        sleep(1);
    }
}
#endif

void histc_generator(pt_histc_t* histc) {
    gint i = 0;
    memset((void*)histc, 0, sizeof(pt_histc_t));
    histc->number = 0;
    srand(time(NULL));
    for (i = 0; i<MAX_RESPONSE_TIME_HIST_LEVELS_PT; i++) {
        histc->response_num[i] = rand()%10 + 1;
        histc->number+=histc->response_num[i];
    }
}

#ifdef __GENERATOR
void data_generator() {
    gint i = 0;
    //second
    pt_histc_t tmp = {0};
    histc_generator(&tmp);
    pt_queue_push(&percentile.sec_statis,tmp);
    //min
    for (i = 0;i<MAX_DATA_NUM_MIN;i++) {
        histc_generator(&tmp);
        pt_queue_push(&percentile.min_statis,tmp);
    }
    //hour
    for (i=0;i<MAX_DATA_NUM_HOUR;i++) {
        histc_generator(&tmp);
        pt_queue_push(&percentile.hor_statis,tmp);
    }
}
#endif

#ifdef __MONITOR
void histc_snap_shot(char* flag, pt_histc_t histc) {
    g_log_dbproxy(g_message, "%s$$#sum=%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d#\n",flag,histc.number
            ,histc.response_num[0],histc.response_num[1],histc.response_num[2],histc.response_num[3],histc.response_num[4],histc.response_num[5],histc.response_num[6],histc.response_num[7],histc.response_num[8],histc.response_num[9]
            ,histc.response_num[10],histc.response_num[11],histc.response_num[12],histc.response_num[13],histc.response_num[14],histc.response_num[15],histc.response_num[16],histc.response_num[17],histc.response_num[18],histc.response_num[19]
            ,histc.response_num[20],histc.response_num[21],histc.response_num[22]);
}
void data_snap_shot(char* flag) {
    int shot[1+MAX_DATA_NUM_MIN+MAX_DATA_NUM_HOUR] = {0};
    shot[0] = percentile->sec_statis.base[0].number;

    pt_queue_pop_mb(percentile->min_statis,MAX_DATA_NUM_MIN,1,shot);
    pt_queue_pop_mb(percentile->hor_statis,MAX_DATA_NUM_HOUR,1 + MAX_DATA_NUM_MIN,shot);

    g_log_dbproxy(g_message, "%s@@#%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-"
            "%d-%d-%d-%d-%d-%d-%d-%d-%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d#\n",flag,shot[0],
            shot[1],shot[2],shot[3],shot[4],shot[5],shot[6],shot[7],shot[8],shot[9],shot[10]
            ,shot[11],shot[12],shot[13],shot[14],shot[15],shot[16],shot[17],shot[18],shot[19],shot[20]
            ,shot[21],shot[22],shot[23],shot[24],shot[25],shot[26],shot[27],shot[28],shot[29],shot[30]
            ,shot[31],shot[32],shot[33],shot[34],shot[35],shot[36],shot[37],shot[38],shot[39],shot[40]
            ,shot[41],shot[42],shot[43],shot[44],shot[45],shot[46],shot[47],shot[48],shot[49],shot[50]
            ,shot[51],shot[52],shot[53],shot[54],shot[55],shot[56],shot[57],shot[58],shot[59],shot[60]
            ,shot[61],shot[62],shot[63],shot[64],shot[65],shot[66],shot[67],shot[68],shot[69],shot[70]
            ,shot[71],shot[72],shot[73],shot[74],shot[75],shot[76],shot[77],shot[78],shot[79],shot[80]
            ,shot[81],shot[82]);
}
void show_percentile_data() {

    int shot[1+MAX_DATA_NUM_MIN+MAX_DATA_NUM_HOUR] = {0};
    shot[0] = percentile->sec_statis.base[0].number;
    pt_histc_t histc;

    while (!chassis_is_shutdown()) {

        system("clear");

        memset(shot, 0, sizeof(int)*(1+MAX_DATA_NUM_MIN+MAX_DATA_NUM_HOUR));

        shot[0] = percentile->sec_statis.base[0].number;
        pt_queue_pop_mb(percentile->min_statis,MAX_DATA_NUM_MIN,1,shot);
        pt_queue_pop_mb(percentile->hor_statis,MAX_DATA_NUM_HOUR,1 + MAX_DATA_NUM_MIN,shot);

        g_print("#%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-"
                "%d-%d-%d-%d-%d-%d-%d-%d-%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d#\n",shot[0],
                shot[1],shot[2],shot[3],shot[4],shot[5],shot[6],shot[7],shot[8],shot[9],shot[10]
                ,shot[11],shot[12],shot[13],shot[14],shot[15],shot[16],shot[17],shot[18],shot[19],shot[20]
                ,shot[21],shot[22],shot[23],shot[24],shot[25],shot[26],shot[27],shot[28],shot[29],shot[30]
                ,shot[31],shot[32],shot[33],shot[34],shot[35],shot[36],shot[37],shot[38],shot[39],shot[40]
                ,shot[41],shot[42],shot[43],shot[44],shot[45],shot[46],shot[47],shot[48],shot[49],shot[50]
                ,shot[51],shot[52],shot[53],shot[54],shot[55],shot[56],shot[57],shot[58],shot[59],shot[60]
                ,shot[61],shot[62],shot[63],shot[64],shot[65],shot[66],shot[67],shot[68],shot[69],shot[70]
                ,shot[71],shot[72],shot[73],shot[74],shot[75],shot[76],shot[77],shot[78],shot[79],shot[80]
                ,shot[81],shot[82]);
        histc = percentile->sec_statis.base[0];
        g_print("\n#sum=%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d#\n",histc.number
                ,histc.response_num[0],histc.response_num[1],histc.response_num[2],histc.response_num[3],histc.response_num[4],histc.response_num[5],histc.response_num[6],histc.response_num[7],histc.response_num[8],histc.response_num[9]
                ,histc.response_num[10],histc.response_num[11],histc.response_num[12],histc.response_num[13],histc.response_num[14],histc.response_num[15],histc.response_num[16],histc.response_num[17],histc.response_num[18],histc.response_num[19]
                ,histc.response_num[20],histc.response_num[21],histc.response_num[22]);
        sleep(1);
    }
}
#define MAX_BUFFER 32
void data_inject() {
    const gchar* fifo_name = "/tmp/percentile_fifo";
    gint pipe_fd = -1;
    gint res = 0;
    gint response_time = 0;
    char buffer[MAX_BUFFER+1] = {""};
    pipe_fd = open(fifo_name,O_RDONLY);
    while (!chassis_is_shutdown()) {
        if (-1 != pipe_fd) {
            do {
                memset(buffer,0,MAX_BUFFER+1);
                res = read(pipe_fd,buffer,MAX_BUFFER);
                response_time = atoi(buffer);
                if (0!=response_time) {
                    pt_percentile_update(pow(2,response_time),&percentile);
                }
            } while (res > 0);
        }
        sleep(1);
    }
    close(pipe_fd);
}
//for unit-test
void histc_snap_shot_console(char* flag, pt_histc_t histc) {//打印直方图
    g_print("%s$$#sum=%d#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-%d#\n",flag,histc.number
            ,histc.response_num[0],histc.response_num[1],histc.response_num[2],histc.response_num[3],histc.response_num[4],histc.response_num[5],histc.response_num[6],histc.response_num[7],histc.response_num[8],histc.response_num[9]
            ,histc.response_num[10],histc.response_num[11],histc.response_num[12],histc.response_num[13],histc.response_num[14],histc.response_num[15],histc.response_num[16],histc.response_num[17],histc.response_num[18],histc.response_num[19]
            ,histc.response_num[20],histc.response_num[21],histc.response_num[22]);
}
void data_snap_shot_console() {
    gint shot[1+MAX_DATA_NUM_MIN] = {0};

    pt_queue_pop_mb(percentile->min_statis, MAX_DATA_NUM_MIN, 1, shot);

    g_print("#%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d,%d-%d-%d-%d-%d-%d-%d-%d-%d-%d-"
            "%d-%d-%d-%d-%d-%d-%d-%d-%d#\n",
            shot[1],shot[2],shot[3],shot[4],shot[5],shot[6],shot[7],shot[8],shot[9],shot[10]
            ,shot[11],shot[12],shot[13],shot[14],shot[15],shot[16],shot[17],shot[18],shot[19],shot[20]
            ,shot[21],shot[22],shot[23],shot[24],shot[25],shot[26],shot[27],shot[28],shot[29],shot[30]
            ,shot[31],shot[32],shot[33],shot[34],shot[35],shot[36],shot[37],shot[38],shot[39],shot[40]
            ,shot[41],shot[42],shot[43],shot[44],shot[45],shot[46],shot[47],shot[48],shot[49],shot[50]
            ,shot[51],shot[52],shot[53],shot[54],shot[55],shot[56],shot[57],shot[58],shot[59]
            );
}
void unit_test(){
    //test arg
    pt_histc_t histc;
    gint no1 = 0;
    gint no2 = 0;
    gint num = 0;
    gint pos = 0;
    gint b[59] = {0};
    gint id = 0;
    gchar *delim = "-";
    gint tx = 0;
    const gchar* fifo_name = "/tmp/unit_test";
    gint pipe_fd = -1;
    gint res = 0;
    gint response_time = 0;
    char buffer[MAX_BUFFER+1] = {""};
    pipe_fd = open(fifo_name,O_RDONLY);
    char* ap;
    while (!chassis_is_shutdown()) {

        if (-1 != pipe_fd) {
            do {
                res = 0;
                memset(buffer,0,MAX_BUFFER+1);
                res = read(pipe_fd,buffer,MAX_BUFFER);
                printf("##command = %s\n",buffer);
                ap = strtok(buffer,delim);
                if(NULL == ap) break;
                id = atoi(ap);

                data_snap_shot_console();

                switch (id) {
                case 1: {
                    pt_queue_new(&percentile->min_statis,MAX_DATA_NUM_MIN);
                    printf("rear:%d front:%d max_size:%d\n",percentile->min_statis.rear,percentile->min_statis.front,percentile->min_statis.max_size-1);
                    break ;
                }
                case 2: {
                    if (pt_is_queue_empty(percentile->min_statis)) {
                        printf("数组为空!\n");
                    } else {
                        printf("数组不为空!\n");
                    }
                    printf("front = %d rear = %d \n",percentile->min_statis.front,percentile->min_statis.rear);
                    break ;
                }
                case 3: {
                    if (pt_is_queue_full(percentile->min_statis)) {
                        printf("数组为满!\n");
                    } else {
                        printf("数组不为满!\n");
                    }
                    printf("front = %d rear = %d \n",percentile->min_statis.front,percentile->min_statis.rear);
                    break ;
                }
                case 4: {
                    memset((void*)&histc,0,sizeof(pt_histc_t));
                    histc_generator(&histc);
                    pt_queue_push(&percentile->min_statis,histc);
                    histc_snap_shot_console("before:",histc);
                    printf("写入后内存:\n");
                    data_snap_shot_console();
                    printf("写入后:\n");
                    printf("rear:%d front:%d\n",percentile->min_statis.rear,percentile->min_statis.front);
                    break ;
                }
                case 5: {
                    memset((void*)&histc,0,sizeof(pt_histc_t));
                    pt_queue_pop(percentile->min_statis,&histc);
                    histc_snap_shot_console("get:",histc);
                    printf("读出后:\n");
                    printf("rear:%d front:%d\n",percentile->min_statis.rear,percentile->min_statis.front);
                    break ;
                }
                case 6: {
                    ap= strtok(NULL,delim);
                    if(NULL == ap) break;
                    no1 = atoi(ap);
                    ap = strtok(NULL,delim);
                    if(NULL == ap) break;
                    no2 = atoi(ap);
                    histc_snap_shot_console("before:",percentile->min_statis.base[no1]);
                    printf("==%d==\n",percentile->min_statis.base[no1].response_num[no2]);
                    printf("arg::%d:%d\n",no1,no2);
                    pt_queue_push_ex(&percentile->min_statis,no1,no2);
                    printf("==%d==\n",percentile->min_statis.base[no1].response_num[no2]);
                    histc_snap_shot_console("after:",percentile->min_statis.base[no1]);
                    printf("写入后内存:\n");
                    data_snap_shot_console();
                    break ;
                }
                case 7: {
                    ap = strtok(NULL,delim);
                    if(NULL==ap) break;
                    num = atoi(ap);
                    memset((void*)&histc,0,sizeof(pt_histc_t));
                    pt_queue_pop_ex(percentile->min_statis,num,&histc);
                    histc_snap_shot_console("get:",histc);
                    break;
                }
                case 8: {
                    ap = strtok(NULL,delim);
                    if (NULL==ap) break;
                    num = atoi(ap);
                    memset((void*)b,0,59);
                    pt_queue_pop_mb(percentile->min_statis,num,0,b);
                    for (tx=0;tx<59;tx++) {
                        printf("%d ",b[tx]);
                    }
                    printf("\n");
                    break;
                }
                }//~switch

            } while (res > 0);//~do

        }//~if
        sleep(1);
    }//~while
    close(pipe_fd);
}
#endif
