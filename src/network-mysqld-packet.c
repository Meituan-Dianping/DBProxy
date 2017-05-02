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
/**
 * codec's for the MySQL client protocol
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "network-mysqld-packet.h"
#include "network_mysqld_type.h"
#include "network_mysqld_proto_binary.h"

#include "glib-ext.h"
#include "../lib/sql-tokenizer.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define COM_ERR_SKIP_HEADER_FAILED  0
#define COM_ERR_GET_INT8_FAILED     1
#define COM_DEBUG_QUERY_STATUS      2
#define COM_INVALID_QUERY_STATUS    3
#define COM_INVALID_COM_TYPE        4

#define EMIT_SIMPLE_CRITICAL(msg) g_critical("%s(%s): event_thread(%d) %s ",    \
                                    G_STRLOC, __func__,                         \
                                    chassis_event_get_threadid(), msg)

const COM_STRING com_command_name[]={
    { C("Sleep") },
    { C("Quit") },
    { C("Init DB") },
    { C("Query") },
    { C("Field List") },
    { C("Create DB") },
    { C("Drop DB") },
    { C("Refresh") },
    { C("Shutdown") },
    { C("Statistics") },
    { C("Processlist") },
    { C("Connect") },
    { C("Kill") },
    { C("Debug") },
    { C("Ping") },
    { C("Time") },
    { C("Delayed insert") },
    { C("Change user") },
    { C("Binlog Dump") },
    { C("Table Dump") },
    { C("Connect Out") },
    { C("Register Slave") },
    { C("Prepare") },
    { C("Execute") },
    { C("Long Data") },
    { C("Close stmt") },
    { C("Reset stmt") },
    { C("Set option") },
    { C("Fetch") },
    { C("Daemon") },
    { C("Binlog Dump GTID") },
    { C("Error") }  // Last command number
};


gboolean sql_token_equal(sql_token *token, sql_token_id token_id, const char *text)
{
    if( token->token_id != token_id)
        return FALSE;
    if(token_id == TK_LITERAL) {
        g_string_ascii_up(token->text);
        if( 0 != strcmp(token->text->str, text) ) 
            return FALSE;
    }
    return TRUE;
}

network_mysqld_com_query_result_t *network_mysqld_com_query_result_new() {
    network_mysqld_com_query_result_t *com_query;

    com_query = g_new0(network_mysqld_com_query_result_t, 1);
    com_query->state = PARSE_COM_QUERY_INIT;
    com_query->query_status = MYSQLD_PACKET_NULL; /* can have 3 values: NULL for unknown, OK for a OK packet, ERR for a error-packet */

    return com_query;
}

void network_mysqld_com_query_result_free(network_mysqld_com_query_result_t *udata) {
    if (!udata) return;

    g_free(udata);
}

/**
 * unused
 *
 * @deprecated will be removed in 0.9
 * @see network_mysqld_proto_get_com_query_result
 */
int network_mysqld_com_query_result_track_state(network_packet G_GNUC_UNUSED *packet, network_mysqld_com_query_result_t G_GNUC_UNUSED *udata) {
    g_log_dbproxy(g_error, "this function is deprecated and network_mysqld_proto_get_com_query_result() should be used instead");
}

static void
query_result_msg_handle(network_mysqld_con *con, gint msg_type, gint com_type,
                                        gint status, gint query_state,
                                        const gchar *strloc, const gchar *func)
{
    GString *msg = g_string_new(NULL);
    switch(msg_type) {
        case COM_ERR_SKIP_HEADER_FAILED:
            {
                g_string_append_printf(msg, " get query result header failed");
                break;
            }
        case COM_ERR_GET_INT8_FAILED:
            {
                g_string_append_printf(msg, " get query result status failed at state %d", query_state);
                break;
            }
        case COM_DEBUG_QUERY_STATUS:
            {
                if (TRACE_SQL(con->srv->log->log_trace_modules)) {
                    g_string_append_printf(msg, " get query result status  %02x at state %d",
                                                (guint8)status, query_state);
                }
                break;
            }
        case COM_INVALID_QUERY_STATUS:
            {
                g_string_append_printf(msg, " get invalid query result %02x at state %d",
                                                     (guint8)status, query_state);
                break;
            }
        case COM_INVALID_COM_TYPE:
            {
                g_string_append_printf(msg, " get invalid com_type at state %d", query_state);
                break;
            }
        default:
            break;
    }

    if (msg_type != COM_DEBUG_QUERY_STATUS) {
        CON_MSG_HANDLE_INTERNAL(g_critical, con, msg->str, strloc, func);
    } else {
        if (TRACE_SQL(con->srv->log->log_trace_modules)) {
            CON_MSG_HANDLE_INTERNAL(g_message, con, msg->str, strloc, func);
        }
    }
    g_string_free(msg, TRUE);
}

/**
 * @return -1 on error
 *         0  on success and done
 *         1  on success and need more
 */
int network_mysqld_proto_get_com_query_result(network_packet *packet, network_mysqld_com_query_result_t *query, network_mysqld_con *con, gboolean use_binary_row_data) {
    int is_finished = 0;
    guint8 status;
    int err = 0;
    network_mysqld_eof_packet_t *eof_packet;
    network_mysqld_ok_packet_t *ok_packet;

    /**
     * if we get a OK in the first packet there will be no result-set
     */
    switch (query->state) {
    case PARSE_COM_QUERY_INIT:
        err = network_mysqld_proto_peek_int8(packet, &status);
        if (err != 0) 
        {
                       query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                con->parse.command, status,
                                query->state, G_STRLOC, __func__);
                       break;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                            con->parse.command, status,
                            query->state, G_STRLOC, __func__);

        switch (status) {
        case MYSQLD_PACKET_ERR: /* e.g. SELECT * FROM dual -> ERROR 1096 (HY000): No tables used */
            query->query_status = MYSQLD_PACKET_ERR;
            is_finished = 1;
            break;
        case MYSQLD_PACKET_OK:  /* e.g. DELETE FROM tbl */

            query->query_status = MYSQLD_PACKET_OK;

            ok_packet = network_mysqld_ok_packet_new();

            err = network_mysqld_proto_get_ok_packet(packet, ok_packet);

            if (!err) {
                if (ok_packet->server_status & SERVER_MORE_RESULTS_EXISTS) {
            
                } else {
                    is_finished = 1;
                }

                query->server_status = ok_packet->server_status;
                query->warning_count = ok_packet->warnings;
                query->affected_rows = ok_packet->affected_rows;
                query->insert_id     = ok_packet->insert_id;
                query->was_resultset = 0;
                query->binary_encoded= use_binary_row_data; 
            }

            network_mysqld_ok_packet_free(ok_packet);

            break;
        case MYSQLD_PACKET_NULL:
            /* OH NO, LOAD DATA INFILE :) */
            query->state = PARSE_COM_QUERY_LOCAL_INFILE_DATA;
            is_finished = 1;

            break;
        case MYSQLD_PACKET_EOF:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            err = 1;

            break;
        default:
            query->query_status = MYSQLD_PACKET_OK;
            /* looks like a result */
            query->state = PARSE_COM_QUERY_FIELD;
            break;
        }
        break;
    case PARSE_COM_QUERY_FIELD:
        err = network_mysqld_proto_peek_int8(packet, &status);
        if (err != 0)
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            break;
        }

        switch (status) {
        case MYSQLD_PACKET_ERR:
        case MYSQLD_PACKET_OK:
        case MYSQLD_PACKET_NULL:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            err = 1;

            break;
        case MYSQLD_PACKET_EOF:
            /**
             * in 5.0 we have CURSORs which have no rows, just a field definition
             *
             * TODO: find a test-case for it, is it COM_STMT_EXECUTE only?
             */
            if (packet->data->len == 9) {
                eof_packet = network_mysqld_eof_packet_new();

                err = network_mysqld_proto_get_eof_packet(packet, eof_packet);

                if (!err) {
#if MYSQL_VERSION_ID >= 50000
                    /* 5.5 may send a SERVER_MORE_RESULTS_EXISTS as part of the first 
                     * EOF together with SERVER_STATUS_CURSOR_EXISTS. In that case,
                     * we aren't finished. (#61998)
                     *
                     * Only if _CURSOR_EXISTS is set alone, we have a field-definition-only
                     * resultset
                     */
                    if (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS &&
                        !(eof_packet->server_status & SERVER_MORE_RESULTS_EXISTS)) {
                        is_finished = 1;
                    } else {
                        query->state = PARSE_COM_QUERY_RESULT;
                    }
#else
                    query->state = PARSE_COM_QUERY_RESULT;
#endif
                    /* track the server_status of the 1st EOF packet */
                    query->server_status = eof_packet->server_status;
                }

                network_mysqld_eof_packet_free(eof_packet);
            } else {
                query->state = PARSE_COM_QUERY_RESULT;
            }
            break;
        default:
            break;
        }
        break;
    case PARSE_COM_QUERY_RESULT:
        err = network_mysqld_proto_peek_int8(packet, &status);
        if (err != 0) 
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            break;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS, con->parse.command,
                                     status, query->state, G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_EOF:
            if (packet->data->len == 9) {
                eof_packet = network_mysqld_eof_packet_new();

                err = network_mysqld_proto_get_eof_packet(packet, eof_packet);
                if (err == 0) {
                    query->was_resultset = 1;

#ifndef SERVER_PS_OUT_PARAMS
#define SERVER_PS_OUT_PARAMS 4096
#endif
                    /**
                     * a PS_OUT_PARAMS is set if a COM_STMT_EXECUTE executes a CALL sp(?) where sp is a PROCEDURE with OUT params 
                     *
                     * ...
                     * 05 00 00 12 fe 00 00 0a 10 -- end column-def (auto-commit, more-results, ps-out-params)
                     * ...
                     * 05 00 00 14 fe 00 00 02 00 -- end of rows (auto-commit), see the missing (more-results, ps-out-params)
                     * 07 00 00 15 00 00 00 02 00 00 00 -- OK for the CALL
                     *
                     * for all other resultsets we trust the status-flags of the 2nd EOF packet
                     */
                    if (!(query->server_status & SERVER_PS_OUT_PARAMS)) {
                        query->server_status = eof_packet->server_status;
                    }
                    query->warning_count = eof_packet->warnings;

                    if (query->server_status & SERVER_MORE_RESULTS_EXISTS) {
                        query->state = PARSE_COM_QUERY_INIT;
                    } else {
                        is_finished = 1;
                    }
                }

                network_mysqld_eof_packet_free(eof_packet);
            }

            break;
        case MYSQLD_PACKET_ERR:
            /* like 
             * 
             * EXPLAIN SELECT * FROM dual; returns an error
             * 
             * EXPLAIN SELECT 1 FROM dual; returns a result-set
             * */
            is_finished = 1;
            break;
        case MYSQLD_PACKET_OK:
        case MYSQLD_PACKET_NULL:
            if (use_binary_row_data) {
                /* fallthrough to default:
                   0x00 is part of the protocol for binary row packets
                 */
            } else {
                /* the first field might be a NULL for a text row packet */
                break;
            }
        default:
            query->rows++;
            query->bytes += packet->data->len;
            break;
        }
        break;
    case PARSE_COM_QUERY_LOCAL_INFILE_DATA: 
        /* we will receive a empty packet if we are done */
        if (packet->data->len == packet->offset) {
            query->state = PARSE_COM_QUERY_LOCAL_INFILE_RESULT;
            is_finished = 1;
        }
        break;
    case PARSE_COM_QUERY_LOCAL_INFILE_RESULT:
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0)
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            break;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status,
                                query->state, G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_OK:
            is_finished = 1;
            break;
        case MYSQLD_PACKET_NULL:
        case MYSQLD_PACKET_ERR:
        case MYSQLD_PACKET_EOF:
        default:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                    con->parse.command, status,
                                    query->state, G_STRLOC, __func__);
            err = 1;

            break;
        }

        break;
    }

    if (err != 0) return -1;

    return is_finished;
}

/**
 * check if the we are in the LOCAL INFILE 'send data from client' state
 *
 * is deprecated as the name doesn't reflect its purpose:
 * - it isn't triggered for LOAD DATA INFILE (w/o LOCAL)
 * - it also covers LOAD XML LOCAL INFILE
 *
 * @deprecated use network_mysqld_com_query_result_is_local_infile() instead
 */
gboolean network_mysqld_com_query_result_is_load_data(network_mysqld_com_query_result_t *udata) {
    return network_mysqld_com_query_result_is_local_infile(udata);
}

/**
 * check if the we are in the LOCAL INFILE 'send data from client' state
 */
gboolean network_mysqld_com_query_result_is_local_infile(network_mysqld_com_query_result_t *udata) {
    return (udata->state == PARSE_COM_QUERY_LOCAL_INFILE_DATA) ? TRUE : FALSE;
}

network_mysqld_com_stmt_prepare_result_t *network_mysqld_com_stmt_prepare_result_new() {
    network_mysqld_com_stmt_prepare_result_t *udata;

    udata = g_new0(network_mysqld_com_stmt_prepare_result_t, 1);
    udata->first_packet = TRUE;

    return udata;
}

void network_mysqld_com_stmt_prepare_result_free(network_mysqld_com_stmt_prepare_result_t *udata) {
    if (!udata) return;

    g_free(udata);
}

int network_mysqld_proto_get_com_stmt_prepare_result(
        network_packet *packet, 
        network_mysqld_com_stmt_prepare_result_t *udata) {
    guint8 status;
    int is_finished = 0;
    int err = 0;

    err = network_mysqld_proto_get_int8(packet, &status);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get Prepare execute status failed");
        return -1;
    }

    g_log_dbproxy(g_debug, "%s execute result is %02x", GET_COM_NAME(COM_STMT_PREPARE), status);

    if (udata->first_packet == 1) {
        udata->first_packet = 0;

        switch (status) {
        case MYSQLD_PACKET_OK:
            g_assert(packet->data->len == 12 + NET_HEADER_SIZE); 

            /* the header contains the number of EOFs we expect to see
             * - no params -> 0
             * - params | fields -> 1
             * - params + fields -> 2 
             */
            udata->want_eofs = 0;

            if (packet->data->str[NET_HEADER_SIZE + 5] != 0 || packet->data->str[NET_HEADER_SIZE + 6] != 0) {
                udata->want_eofs++;
            }
            if (packet->data->str[NET_HEADER_SIZE + 7] != 0 || packet->data->str[NET_HEADER_SIZE + 8] != 0) {
                udata->want_eofs++;
            }

            if (udata->want_eofs == 0) {
                is_finished = 1;
            }

            break;
        case MYSQLD_PACKET_ERR:
            is_finished = 1;
            break;
        default:
            g_log_dbproxy(g_warning, "%s get invalid status: %02x", GET_COM_NAME(COM_STMT_PREPARE), status);
            break;
        }
    } else {
        switch (status) {
        case MYSQLD_PACKET_OK:
        case MYSQLD_PACKET_NULL:
        case MYSQLD_PACKET_ERR:
            g_log_dbproxy(g_error, "%s should not be (OK|ERR|NULL), got: %02x", GET_COM_NAME(COM_STMT_PREPARE), status);
            break;
        case MYSQLD_PACKET_EOF:
            if (--udata->want_eofs == 0) {
                is_finished = 1;
            }

            break;
        default:
            break;
        }
    }

    if (err != 0) return -1;

    return is_finished;
}

guint8 *network_mysqld_com_result_new() {
    guint8 *udata;

    udata = g_new0(guint8, 1);
    *udata = MYSQLD_PACKET_NULL;

    return udata;
}

void network_mysqld_com_result_free(guint8 *udata) {
    if (!udata) return;

    g_free(udata);
}

/**
 * init the tracking of the sub-states of the protocol
 */
int network_mysqld_con_command_states_init(network_mysqld_con *con, network_packet *packet) {
    guint8 cmd;

    if (network_mysqld_proto_skip_network_header(packet) != 0) {
        CON_MSG_HANDLE(g_critical, con, "get network-header failed");
        return -1;
    }

    if (network_mysqld_proto_get_int8(packet, &cmd) != 0) {
        CON_MSG_HANDLE(g_critical, con, "get 1-byte comman type failed");
        return -1;
    }

    con->parse.command = cmd;

    packet->offset = 0; /* reset the offset again for the next functions */

    /* init the parser for the commands */
    switch (con->parse.command) {
    case COM_QUERY:
    case COM_PROCESS_INFO:
    case COM_STMT_EXECUTE:
        con->parse.data = network_mysqld_com_query_result_new();
        con->parse.data_free = (GDestroyNotify)network_mysqld_com_query_result_free;
        break;
    case COM_STMT_PREPARE:
        con->parse.data = network_mysqld_com_stmt_prepare_result_new();
        con->parse.data_free = (GDestroyNotify)network_mysqld_com_stmt_prepare_result_free;
        break;
    case COM_INIT_DB:
    case COM_CHANGE_USER:
        // 保存执行结果
        con->parse.data = network_mysqld_com_result_new();
        con->parse.data_free = (GDestroyNotify)network_mysqld_com_result_free;
        break;
    case COM_QUIT:
        /* track COM_QUIT going to the server, to be able to tell if the server
         * a) simply went away or
         * b) closed the connection because the client asked it to
         * If b) we should not print a message at the next EV_READ event from the server fd
         */
        con->com_quit_seen = TRUE;
    default:
        break;
    }

    return 0;
}

/**
 * @param packet the current packet that is passing by
 *
 *
 * @return -1 on invalid packet, 
 *          0 need more packets, 
 *          1 for the last packet 
 */
int network_mysqld_proto_get_query_result(network_packet *packet, network_mysqld_con *con) {
    guint8 status;
    int is_finished = 0;
    int err = 0;
    network_mysqld_eof_packet_t *eof_packet;
    
    err = network_mysqld_proto_skip_network_header(packet);
    if (err != 0 ) {
        query_result_msg_handle(con, COM_ERR_SKIP_HEADER_FAILED,
                                    0, 0, -1, G_STRLOC, __func__);
        return -1;
    }

    /* forward the response to the client */
    switch (con->parse.command) {
    case COM_CHANGE_USER: 
        /**
         * - OK
         * - ERR (in 5.1.12+ + a duplicate ERR)
         */
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0) {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, 0, -1,
                                    G_STRLOC, __func__);
            return -1;
        }
        *((guint8 *)con->parse.data) = status;

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);

        switch (status) {
        case MYSQLD_PACKET_ERR:
            is_finished = 1;
            break;
        case MYSQLD_PACKET_OK:
            is_finished = 1;
            break;
        default:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
            break;
        }
        break;
    case COM_INIT_DB:
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0)
        {       
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
            return -1;
        }
        *((guint8 *)con->parse.data) = status;

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_ERR:
            is_finished = 1;
            break;
        case MYSQLD_PACKET_OK:
            is_finished = 1;
            break;
        default:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
            break;
        }

        break;
    case COM_REFRESH:
    case COM_STMT_RESET:
    case COM_PING:
    case COM_TIME:
    case COM_REGISTER_SLAVE:
    case COM_PROCESS_KILL:
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0)
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, status, -1,
                                    G_STRLOC, __func__);
            return -1;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_ERR:
        case MYSQLD_PACKET_OK:
            is_finished = 1;
            break;
        default:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                    con->parse.command, status, -1,
                                    G_STRLOC, __func__);
            break;
        }
        break;
    case COM_DEBUG:
    case COM_SET_OPTION:
    case COM_SHUTDOWN:
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0)
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                     con->parse.command, status, -1,
                                     G_STRLOC, __func__);
            return -1;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_ERR: /* COM_DEBUG may not have the right permissions */
        case MYSQLD_PACKET_EOF:
            is_finished = 1;
            break;
        default:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                    con->parse.command, status, -1,
                                    G_STRLOC, __func__);
            break;
        }
        break;

    case COM_FIELD_LIST:
        err = network_mysqld_proto_get_int8(packet, &status);
        if (err != 0)
        {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                   con->parse.command, status, -1,
                                   G_STRLOC, __func__);
            return -1;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, -1,
                                G_STRLOC, __func__);
        /* we transfer some data and wait for the EOF */
        switch (status) {
        case MYSQLD_PACKET_ERR:
        case MYSQLD_PACKET_EOF:
            is_finished = 1;
            break;
        case MYSQLD_PACKET_NULL:
        case MYSQLD_PACKET_OK:
            query_result_msg_handle(con, COM_INVALID_QUERY_STATUS,
                                        con->parse.command, status, -1,
                                        G_STRLOC, __func__);
            break;
        default:
            break;
        }
        break;
#if MYSQL_VERSION_ID >= 50000
    case COM_STMT_FETCH:
        err = network_mysqld_proto_peek_int8(packet, &status);
        if (err != 0) {
            query_result_msg_handle(con, COM_ERR_GET_INT8_FAILED,
                                    con->parse.command, status, 0,
                                    G_STRLOC, __func__);
            return -1;
        }

        query_result_msg_handle(con, COM_DEBUG_QUERY_STATUS,
                                con->parse.command, status, 0,
                                G_STRLOC, __func__);
        switch (status) {
        case MYSQLD_PACKET_EOF: 
            eof_packet = network_mysqld_eof_packet_new();

            err = network_mysqld_proto_get_eof_packet(packet, eof_packet);
            if (err == 0) {
                if ((eof_packet->server_status & SERVER_STATUS_LAST_ROW_SENT) ||
                    (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS)) {
                    is_finished = 1;
                }
            }

            network_mysqld_eof_packet_free(eof_packet);

            break; 
        case MYSQLD_PACKET_ERR:
            is_finished = 1;

            break;
        default:
            break;
        }
        break;
#endif
    case COM_QUIT: /* sometimes we get a packet before the connection closes */
    case COM_STATISTICS:
        /* just one packet, no EOF */
        is_finished = 1;

        break;
    case COM_STMT_PREPARE:
        is_finished = network_mysqld_proto_get_com_stmt_prepare_result(packet, con->parse.data);
        break;
    case COM_STMT_EXECUTE:
        /* COM_STMT_EXECUTE result packets are basically the same as COM_QUERY ones,
         * the only difference is the encoding of the actual data - fields are in there, too.
         */
        is_finished = network_mysqld_proto_get_com_query_result(packet, con->parse.data, con, TRUE);
        break;
    case COM_PROCESS_INFO:
    case COM_QUERY:
        is_finished = network_mysqld_proto_get_com_query_result(packet, con->parse.data, con, FALSE);
        break;
    case COM_BINLOG_DUMP:
        /**
         * the binlog-dump event stops, forward all packets as we see them
         * and keep the command active
         */
        is_finished = 1;
        break;
    default:
        query_result_msg_handle(con, COM_INVALID_COM_TYPE,
                                con->parse.command, 0, -1,
                                G_STRLOC, __func__);
        err = 1;
        break;
    }

    if (err != 0) return -1;

    return is_finished;
}

int network_mysqld_proto_get_fielddef(network_packet *packet, network_mysqld_proto_fielddef_t *field, guint32 capabilities) {
    int err = 0;
    gchar *msg = NULL;

    if (capabilities & CLIENT_PROTOCOL_41) {
        guint16 field_charsetnr;
        guint32 field_length;
        guint8 field_type;
        guint16 field_flags;
        guint8 field_decimals;

        err = network_mysqld_proto_get_lenenc_string(packet, &field->catalog, NULL);
        if (err != 0) {
            msg = "get Field's catalog failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->db, NULL);
        if (err != 0) {
            msg = "get Field's dbname failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->table, NULL);
        if (err != 0) {
            msg = "get Field's tblname failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->org_table, NULL);
        if (err != 0) {
            msg = "get Field's ori tblname failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->name, NULL);
        if (err != 0) {
            msg = "get Field's column name failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->org_name, NULL);
        if (err != 0) {
            msg = "get Field's ori column name failed";
            goto funcexit;
        }
        err = network_mysqld_proto_skip(packet, 1); /* filler */
        if (err != 0) {
            msg = "get Field's 1-byte placeholder failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int16(packet, &field_charsetnr);
        if (err != 0) {
            msg = "get Field's charset failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int32(packet, &field_length);
        if (err != 0) {
            msg = "get Field's length failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet,  &field_type);
        if (err != 0) {
            msg = "get Field's type failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int16(packet, &field_flags);
        if (err != 0) {
            msg = "get Field's flag failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet,  &field_decimals);
        if (err != 0) {
            msg = "get Field's precision failed";
            goto funcexit;
        }
        err = network_mysqld_proto_skip(packet, 2); /* filler */
        if (err != 0) {
            msg = "get Field's 2-bytes placeholders failed";
            goto funcexit;
        }
        if (err == 0) {
            field->charsetnr = field_charsetnr;
            field->length    = field_length;
            field->type      = field_type;
            field->flags     = field_flags;
            field->decimals  = field_decimals;
        }
    } else {
        guint8 len;
        guint32 field_length;
        guint8  field_type;
        guint8  field_decimals;

        /* see protocol.cc Protocol::send_fields */

        err = network_mysqld_proto_get_lenenc_string(packet, &field->table, NULL);
        if (err != 0) {
            msg = "get Field's ori tblname failed under pre4.1 protocal";
            goto funcexit;
        }
        err = network_mysqld_proto_get_lenenc_string(packet, &field->name, NULL);
        if (err != 0) {
            msg = "get Field's column failed under pre4.1 protocal";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet, &len);
        if (err != 0 ) {
            msg = "get Field's length's bound failed under pre4.1 protocal";
            goto funcexit;
        }
        if (len != 3) {
            msg = "get Field's packet length's != 3bytes under pre4.1 protocal";
            err = -1;
            goto funcexit;
        }
        err = network_mysqld_proto_get_int24(packet, &field_length);
        if (err != 0) {
            msg = "get Field's length failed under pre4.1 protocal";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet, &len);
        if (err != 0) {
            msg = "get Field's length's bound failed under pre4.1 protocal";
            goto funcexit;
        }

        if (len != 1) {
            msg = "get Field's filed_type length != 1byte under pre4.1 protocal";
            err = -1;
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet, &field_type);
        if (err != 0) {
            msg = "get Field's type failed under pre4.1 protocal";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int8(packet, &len);
        if (err != 0) {
            msg = "get Field's flag's length failed under pre4.1 protocal";
            goto funcexit;
        }
        if (len == 3) { /* the CLIENT_LONG_FLAG is set */
            guint16 field_flags;

            err = network_mysqld_proto_get_int16(packet, &field_flags);
            if (err != 0) {
                msg = "get Field's 2-bytes flag failed under pre4.1 protocal";
                goto funcexit;
            }
            if (!err) field->flags = field_flags;
        } else if (len == 2) {
            guint8 field_flags;

            err = network_mysqld_proto_get_int8(packet, &field_flags);
            if (err != 0) {
                msg = "get Field's 1-byte flag failed under pre4.1 protocal";
                goto funcexit;
            }
            if (!err) field->flags = field_flags;
        } else {
            err = -1;
        }
        err = network_mysqld_proto_get_int8(packet, &field_decimals);
        if (err != 0) {
            msg = "get Field's 1-byte precision value failed under pre4.1 protocal";
            goto funcexit;
        }
        if (err == 0) {
            field->charsetnr = 0x08 /* latin1_swedish_ci */;
            field->length    = field_length;
            field->type      = field_type;
            field->decimals  = field_decimals;
        }
    }

funcexit:

    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}

/**
 * parse the result-set packet and extract the fields
 *
 * @param chunk  list of mysql packets 
 * @param fields empty array where the fields shall be stored in
 *
 * @return NULL if there is no resultset
 *         pointer to the chunk after the fields (to the EOF packet)
 */ 
GList *network_mysqld_proto_get_fielddefs(GList *chunk, GPtrArray *fields) {
    network_packet packet;
    guint64 field_count;
    guint i;
    int err = 0;
    guint32 capabilities = CLIENT_PROTOCOL_41;
    network_mysqld_lenenc_type lenenc_type;
    gchar *errmsg = NULL;
    
    packet.data = chunk->data;
    packet.offset = 0;

    err = network_mysqld_proto_skip_network_header(&packet);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get fielddefs's network header failed");
        return NULL;
    }
    
    err = network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get fielddefs's lenenc_type failed");
        return NULL; /* packet too short */
    }

    /* make sure that we have a valid length-encoded integer here */
    switch (lenenc_type) {
    case NETWORK_MYSQLD_LENENC_TYPE_INT:
        break;
    default:
        /* we shouldn't be here, we expected to get a valid length-encoded field count */
        return NULL;
    }
    
    err = network_mysqld_proto_get_lenenc_int(&packet, &field_count);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get fielddefs's length coded field_count failed");
        return NULL; /* packet too short */
    }

    if (field_count == 0) {
        /* shouldn't happen, the upper layer should have checked that this is a OK packet */
        EMIT_SIMPLE_CRITICAL("fielddefs's field_count == 0 which unexpected");
        return NULL;
    }
    
    /* the next chunk, the field-def */
    for (i = 0; i < field_count; i++) {
        network_mysqld_proto_fielddef_t *field;
        gchar *errmsg = NULL;
        
        chunk = chunk->next;
        g_assert(chunk);

        packet.data = chunk->data;
        packet.offset = 0;

        field = network_mysqld_proto_fielddef_new();

        err = network_mysqld_proto_skip_network_header(&packet);
        if (err != 0) {
            errmsg = g_strdup_printf("get %dth fielddef network header failed", i);
        }

        if (err == 0) {
            err = network_mysqld_proto_get_fielddef(&packet, field, capabilities);
            if (err != 0) {
                errmsg = g_strdup_printf("get %dth fielddef failed", i);
            }
        }

        if (err != 0) {
            network_mysqld_proto_fielddef_free(field);
            EMIT_SIMPLE_CRITICAL(errmsg);
            g_free(errmsg);
            return NULL;
        }

            g_ptr_array_add(fields, field); /* even if we had an error, append it so that we can free it later */
    }
    
    /* this should be EOF chunk */
    chunk = chunk->next;

    if (!chunk) return NULL;

    packet.data = chunk->data;
    packet.offset = 0;
    
    err = network_mysqld_proto_skip_network_header(&packet);
    if (err != 0) {
            errmsg = "get proto network header failed";
            goto funcexit;
    }
    
    err = network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
    if (err != 0) {
            errmsg = "get proto's lenenc type failed";
            goto funcexit;
    }
    if (lenenc_type != NETWORK_MYSQLD_LENENC_TYPE_EOF) {
        err = 1;
        errmsg = "get proto's lenenc type isn't NETWORK_MYSQLD_LENENC_TYPE_EOF";
        goto funcexit;
    }
funcexit:
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(errmsg);
        return NULL;
    }
    return chunk;
}

network_mysqld_ok_packet_t *network_mysqld_ok_packet_new() {
    network_mysqld_ok_packet_t *ok_packet;

    ok_packet = g_new0(network_mysqld_ok_packet_t, 1);

    return ok_packet;
}

void network_mysqld_ok_packet_free(network_mysqld_ok_packet_t *ok_packet) {
    if (!ok_packet) return;

    g_free(ok_packet);
}


/**
 * decode a OK packet from the network packet
 */
int network_mysqld_proto_get_ok_packet(network_packet *packet, network_mysqld_ok_packet_t *ok_packet) {
    guint8 field_count;
    guint64 affected, insert_id;
    guint16 server_status, warning_count = 0;

    int err = 0;
    gchar *msg = NULL;

    err = network_mysqld_proto_get_int8(packet, &field_count);

    if (err != 0)
    {       
        msg = "get ok packet's  status failed";
        goto funcexit;
    }

    if (field_count != 0) {
        g_log_dbproxy(g_warning, "expected the first byte to be 0, got %d", field_count);
        return -1;
    }

    err = network_mysqld_proto_get_lenenc_int(packet, &affected);
    if (err != 0) {
        msg = "get ok packet's affected_rows failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_lenenc_int(packet, &insert_id);
    if (err != 0) {
        msg = "get ok packet's insert_id failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int16(packet, &server_status);
    if (err != 0) {
        msg = "get ok packet's server_status failed";
        goto funcexit;
    } 

    err = network_mysqld_proto_get_int16(packet, &warning_count);
    if (err != 0) {
        msg = "get ok packet's warning_count failed";
        EMIT_SIMPLE_CRITICAL(msg);
        goto funcexit;
    }

    if (err == 0) {
        ok_packet->affected_rows = affected;
        ok_packet->insert_id     = insert_id;
        ok_packet->server_status = server_status;
        ok_packet->warnings      = warning_count;
    }

funcexit:
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }
    return err ? -1 : 0;
}

int network_mysqld_proto_append_ok_packet(GString *packet, network_mysqld_ok_packet_t *ok_packet) {
    guint32 capabilities = CLIENT_PROTOCOL_41;

    network_mysqld_proto_append_int8(packet, 0); /* no fields */
    network_mysqld_proto_append_lenenc_int(packet, ok_packet->affected_rows);
    network_mysqld_proto_append_lenenc_int(packet, ok_packet->insert_id);
    network_mysqld_proto_append_int16(packet, ok_packet->server_status); /* autocommit */
    if (capabilities & CLIENT_PROTOCOL_41) {
        network_mysqld_proto_append_int16(packet, ok_packet->warnings); /* no warnings */
    }

    return 0;
}

static network_mysqld_err_packet_t *network_mysqld_err_packet_new_full(network_mysqld_protocol_t version) {
    network_mysqld_err_packet_t *err_packet;

    err_packet = g_new0(network_mysqld_err_packet_t, 1);
    err_packet->sqlstate = g_string_new(NULL);
    err_packet->errmsg = g_string_new(NULL);
    err_packet->version = version;
    err_packet->errcode = 0;

    return err_packet;
}

network_mysqld_err_packet_t *network_mysqld_err_packet_new() {
    return network_mysqld_err_packet_new_full(NETWORK_MYSQLD_PROTOCOL_VERSION_41);
}

network_mysqld_err_packet_t *network_mysqld_err_packet_new_pre41() {
    return network_mysqld_err_packet_new_full(NETWORK_MYSQLD_PROTOCOL_VERSION_PRE41);
}

void network_mysqld_err_packet_free(network_mysqld_err_packet_t *err_packet) {
    if (!err_packet) return;

    g_string_free(err_packet->sqlstate, TRUE);
    g_string_free(err_packet->errmsg, TRUE);

    g_free(err_packet);
}

/**
 * decode a ERR packet from the network packet
 */
int network_mysqld_proto_get_err_packet(network_packet *packet, network_mysqld_err_packet_t *err_packet) {
    guint8 field_count, marker;
    guint16 errcode;
    gchar *sqlstate = NULL, *errmsg = NULL;
    guint32 capabilities = CLIENT_PROTOCOL_41;

    int err = 0;
    gchar *msg = NULL;

    err = network_mysqld_proto_get_int8(packet, &field_count);
    if (err != 0) 
    {
        msg = "get err packet's status failed";
        goto funcexit;
    }

    if (field_count != MYSQLD_PACKET_ERR) {
        g_log_dbproxy(g_warning, "expected the first byte to be 0xff, got %d", field_count);
        return -1;
    }

    err = network_mysqld_proto_get_int16(packet, &errcode);
    if (err != 0) {
        msg = "get err packet's errcode failed";
        goto funcexit;
    }

    if (capabilities & CLIENT_PROTOCOL_41 && errcode != 1040) {
        err = network_mysqld_proto_get_int8(packet, &marker);
        if (err != 0) {
            msg = "get err packet's marker failed";
            goto funcexit;
        }

        if (marker != '#') {
            msg = "err packet's marker isn't '#'";
            err = -1;
            goto funcexit;
        }

        err = network_mysqld_proto_get_string_len(packet, &sqlstate, 5);
        if (err != 0) {
            msg = "get err packet's sqlstate failed";
            goto funcexit;
        }
    }

    if (packet->offset < packet->data->len) {
        err = network_mysqld_proto_get_string_len(packet, &errmsg, packet->data->len - packet->offset);
        if (err != 0) {
            msg = "get err packet's errmsg failed";
            goto funcexit;
        }
    }

    if (!err) {
        err_packet->errcode = errcode;
        if (errmsg) g_string_assign(err_packet->errmsg, errmsg);
        if (sqlstate) g_string_assign(err_packet->sqlstate, sqlstate);
        if (errmsg) { EMIT_SIMPLE_CRITICAL(errmsg); }
    }

funcexit:

    if (sqlstate) g_free(sqlstate);
    if (errmsg) g_free(errmsg);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}



/**
 * create a ERR packet
 *
 * @note the sqlstate has to match the SQL standard. If no matching SQL state is known, leave it at NULL
 *
 * @param packet      network packet
 * @param err_packet  the error structure
 *
 * @return 0 on success
 */
int network_mysqld_proto_append_err_packet(GString *packet, network_mysqld_err_packet_t *err_packet) {
    int errmsg_len;

    network_mysqld_proto_append_int8(packet, 0xff); /* ERR */
    network_mysqld_proto_append_int16(packet, err_packet->errcode); /* errorcode */
    if (err_packet->version == NETWORK_MYSQLD_PROTOCOL_VERSION_41) {
        g_string_append_c(packet, '#');
        if (err_packet->sqlstate && (err_packet->sqlstate->len > 0)) {
            g_string_append_len(packet, err_packet->sqlstate->str, 5);
        } else {
            g_string_append_len(packet, C("07000"));
        }
    }

    errmsg_len = err_packet->errmsg->len;
    if (errmsg_len >= 512) errmsg_len = 512;
    g_string_append_len(packet, err_packet->errmsg->str, errmsg_len);

    return 0;
}

network_mysqld_eof_packet_t *network_mysqld_eof_packet_new() {
    network_mysqld_eof_packet_t *eof_packet;

    eof_packet = g_new0(network_mysqld_eof_packet_t, 1);

    return eof_packet;
}

void network_mysqld_eof_packet_free(network_mysqld_eof_packet_t *eof_packet) {
    if (!eof_packet) return;

    g_free(eof_packet);
}


/**
 * decode a OK packet from the network packet
 */
int network_mysqld_proto_get_eof_packet(network_packet *packet, network_mysqld_eof_packet_t *eof_packet) {
    guint8 field_count;
    guint16 server_status, warning_count;
    guint32 capabilities = CLIENT_PROTOCOL_41;

    int err = 0;
    gchar *msg = NULL;

    err = network_mysqld_proto_get_int8(packet, &field_count);
    if (err != 0) {
        msg = "get eof packet's status failed";
        goto funcexit;
    }

    if (field_count != MYSQLD_PACKET_EOF) {
        g_log_dbproxy(g_warning, "expected the first byte to be 0xfe, got %d", field_count);
        return -1;
    }

    if (capabilities & CLIENT_PROTOCOL_41) {
        err = network_mysqld_proto_get_int16(packet, &warning_count);
        if (err != 0) {
            msg = "get eof packet's warning_count failed";
            goto funcexit;
        }
        err = network_mysqld_proto_get_int16(packet, &server_status);
        if (err != 0) {
            msg = "get eof packet's server_status failed";
            goto funcexit;
        }
        if (!err) {
            eof_packet->server_status = server_status;
            eof_packet->warnings      = warning_count;
        }
    } else {
        eof_packet->server_status = 0;
        eof_packet->warnings      = 0;
    }

funcexit:
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}

int network_mysqld_proto_append_eof_packet(GString *packet, network_mysqld_eof_packet_t *eof_packet) {
    guint32 capabilities = CLIENT_PROTOCOL_41;

    network_mysqld_proto_append_int8(packet, MYSQLD_PACKET_EOF); /* no fields */
    if (capabilities & CLIENT_PROTOCOL_41) {
        network_mysqld_proto_append_int16(packet, eof_packet->warnings); /* no warnings */
        network_mysqld_proto_append_int16(packet, eof_packet->server_status); /* autocommit */
    }

    return 0;
}


network_mysqld_auth_challenge *network_mysqld_auth_challenge_new() {
    network_mysqld_auth_challenge *shake;

    shake = g_new0(network_mysqld_auth_challenge, 1);
    
    shake->challenge = g_string_sized_new(20);
    shake->capabilities = 
        CLIENT_PROTOCOL_41 |
        CLIENT_SECURE_CONNECTION |
        0;


    return shake;
}

void network_mysqld_auth_challenge_free(network_mysqld_auth_challenge *shake) {
    if (!shake) return;

    if (shake->server_version_str) g_free(shake->server_version_str);
    if (shake->challenge)          g_string_free(shake->challenge, TRUE);

    g_free(shake);
}

void network_mysqld_auth_challenge_set_challenge(network_mysqld_auth_challenge *shake) {
    guint i;

    /* 20 chars */

    g_string_set_size(shake->challenge, 21);

    for (i = 0; i < 20; i++) {
        shake->challenge->str[i] = (94.0 * (rand() / (RAND_MAX + 1.0))) + 33; /* 33 - 127 are printable characters */
    }

    shake->challenge->len = 20;
    shake->challenge->str[shake->challenge->len] = '\0';
}

int network_mysqld_proto_get_auth_challenge(network_packet *packet, network_mysqld_auth_challenge *shake) {
    int maj, min, patch;
    gchar *scramble_1 = NULL, *scramble_2 = NULL;
    guint8 status;
    int err = 0;
    gchar *msg = NULL;

    err = network_mysqld_proto_get_int8(packet, &status);
    if (err != 0) {
        msg = "get auth challenge's status failed";
        goto funcexit;
    }

    switch (status) {
    case 0xff:
        msg = "get auth challenge's is returnning ERR";
        err = -1;
        goto funcexit;
    case 0x0a:
        break;
    default:
        g_log_dbproxy(g_warning, "unknown protocol %d", status);
        msg = "get auth challenge's is returnning neither OK nor ERR";
        err = -1;
        goto funcexit;
    }

    err = network_mysqld_proto_get_string(packet, &shake->server_version_str);
    if (err != 0) {
        msg = "get auth challenge's server_version_str failed";
        goto funcexit;
    }

    if (shake->server_version_str == NULL) { /* the server-version has to be set */
        msg = "auth challenge's server_version_str is null";
        err = -1;
        goto funcexit;
    }

    err = network_mysqld_proto_get_int32(packet, &shake->thread_id);
    if (err != 0) {
        msg = "get auth challenge's thread_id failed";
        goto funcexit;
    }

    /**
     * get the scramble buf
     *
     * 8 byte here and some the other 12 somewhen later
     */ 
    err = network_mysqld_proto_get_string_len(packet, &scramble_1, 8);
    if (err != 0) {
        msg = "get auth challenge's 8-bytes challenge random number failed";
        goto funcexit;
    }
    err = network_mysqld_proto_skip(packet, 1);
    if (err != 0) {
        msg = "get auth challenge's 1-byte placeholder(0x00) failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int16(packet, &shake->capabilities);
    if (err != 0) {
        msg = "get auth challenge's 2-bytes capabilities failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int8(packet, &shake->charset);
    if (err != 0) {
        msg = "get auth challenge's 1-byte charset failed";
        goto funcexit;
    }

    err = network_mysqld_proto_get_int16(packet, &shake->server_status);
    if (err != 0) {
        msg = "get auth challenge's 2-bytes server_status failed";
        goto funcexit;
    }

    err = network_mysqld_proto_skip(packet, 13);
    if (err != 0) {
        msg = "get auth challenge's 13-bytes placeholder(0x00) failed";
    }

    if (shake->capabilities & CLIENT_SECURE_CONNECTION) {
        err = network_mysqld_proto_get_string_len(packet, &scramble_2, 12);
        if (err != 0) {
            msg = "get auth challenge's 12-bytes challenge random number failed";
            goto funcexit;
        }
    }

    err = network_mysqld_proto_skip(packet, 1);
    if (err != 0) {
        msg = "get auth challenge's 1-byte skip number(0x00) failed";
        goto funcexit;
    }

    if (err == 0) {
        /* process the data */
    
        if (3 != sscanf(shake->server_version_str, "%d.%d.%d%*s", &maj, &min, &patch)) {
            /* can't parse the protocol */
            msg = "auth challenge's parser server_version_str failed";
            err = -1;
            goto funcexit;
        }
    
        /**
         * out of range 
         */
        if (min   < 0 || min   > 100 ||
            patch < 0 || patch > 100 ||
            maj   < 0 || maj   > 10) {
            gchar *ver_str = g_strdup_printf("auth challenge's version number out of range : "
                                    "maj=%d, min=%d, patch=%d", maj, min, patch);
            EMIT_SIMPLE_CRITICAL(ver_str);
            g_free(ver_str);
            goto funcexit;
        }
    
        shake->server_version = maj * 10000 + min * 100 + patch;
    
        /**
         * scramble_1 + scramble_2 == scramble
         *
         * a len-encoded string
         */
        g_string_truncate(shake->challenge, 0);
        g_string_append_len(shake->challenge, scramble_1, 8);
        if (scramble_2) g_string_append_len(shake->challenge, scramble_2, 12); /* in old-password, no 2nd scramble */
    }

funcexit:
    if (scramble_1) g_free(scramble_1);
    if (scramble_2) g_free(scramble_2);

    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}

int network_mysqld_proto_append_auth_challenge(GString *packet, network_mysqld_auth_challenge *shake) {
    guint i;

    network_mysqld_proto_append_int8(packet, 0x0a);
    if (shake->server_version_str) {
        g_string_append(packet, shake->server_version_str);
    } else if (shake->server_version > 30000 && shake->server_version < 100000) {
        g_string_append_printf(packet, "%d.%02d.%02d", 
                shake->server_version / 10000,
                (shake->server_version % 10000) / 100,
                shake->server_version %   100
                );
    } else {
        g_string_append_len(packet, C("5.0.99"));
    }
    network_mysqld_proto_append_int8(packet, 0x00);
    network_mysqld_proto_append_int32(packet, shake->thread_id);
    if (shake->challenge->len) {
        g_string_append_len(packet, shake->challenge->str, 8);
    } else {
        g_string_append_len(packet, C("01234567"));
    }
    network_mysqld_proto_append_int8(packet, 0x00); /* filler */
    network_mysqld_proto_append_int16(packet, shake->capabilities);
    network_mysqld_proto_append_int8(packet, shake->charset);
    network_mysqld_proto_append_int16(packet, shake->server_status);

    for (i = 0; i < 13; i++) {
        network_mysqld_proto_append_int8(packet, 0x00);
    }

    if (shake->challenge->len) {
        g_string_append_len(packet, shake->challenge->str + 8, 12);
    } else {
        g_string_append_len(packet, C("890123456789"));
    }
    network_mysqld_proto_append_int8(packet, 0x00);

    return 0;
}

network_mysqld_auth_response *network_mysqld_auth_response_new() {
    network_mysqld_auth_response *auth;

    auth = g_new0(network_mysqld_auth_response, 1);

    /* we have to make sure scramble->buf is not-NULL to get
     * the "empty string" and not a "NULL-string"
     */
    auth->response = g_string_new("");
    auth->username = g_string_new("");
    auth->database = g_string_new("");
    auth->capabilities = CLIENT_SECURE_CONNECTION | CLIENT_PROTOCOL_41;

    return auth;
}

void network_mysqld_auth_response_free(network_mysqld_auth_response *auth) {
    if (!auth) return;

    if (auth->response)          g_string_free(auth->response, TRUE);
    if (auth->username)          g_string_free(auth->username, TRUE);
    if (auth->database)          g_string_free(auth->database, TRUE);

    g_free(auth);
}

int network_mysqld_proto_get_auth_response(network_packet *packet, network_mysqld_auth_response *auth) {
    int err = 0;
    guint16 l_cap;
    gchar *msg = NULL;
    /* extract the default db from it */

    /*
     * @\0\0\1
     *  \215\246\3\0 - client-flags
     *  \0\0\0\1     - max-packet-len
     *  \10          - charset-num
     *  \0\0\0\0
     *  \0\0\0\0
     *  \0\0\0\0
     *  \0\0\0\0
     *  \0\0\0\0
     *  \0\0\0       - fillers
     *  root\0       - username
     *  \24          - len of the scrambled buf
     *    ~    \272 \361 \346
     *    \211 \353 D    \351
     *    \24  \243 \223 \257
     *    \0   ^    \n   \254
     *    t    \347 \365 \244
     *  
     *  world\0
     */


    /* 4.0 uses 2 byte, 4.1+ uses 4 bytes, but the proto-flag is in the lower 2 bytes */
    err = network_mysqld_proto_peek_int16(packet, &l_cap);
    if (err != 0) {
        msg = "get auth response's proto-flag failed";
        goto fun_exit;
    }

    if (l_cap & CLIENT_PROTOCOL_41) {
        err = network_mysqld_proto_get_int32(packet, &auth->capabilities);
        if (err != 0) {
            msg = "get auth response's client capabilities failed";
            goto fun_exit;
        }
        err = network_mysqld_proto_get_int32(packet, &auth->max_packet_size);
        if (err != 0) {
            msg = "get auth response's max_packet_size failed";
            goto fun_exit;
        }
        err = network_mysqld_proto_get_int8(packet, &auth->charset);
        if (err != 0) {
            msg = "get auth response's charset failed";
            goto fun_exit;
        }
        err = network_mysqld_proto_skip(packet, 23);
        if (err != 0) {
            msg = "get auth response's 23-bytes placeholder(0x00) failed";
            goto fun_exit;
        }
        err = network_mysqld_proto_get_gstring(packet, auth->username);
        if (err != 0) {
            msg = "get auth response's username failed";
            goto fun_exit;
        }
        if (auth->capabilities & CLIENT_SECURE_CONNECTION) {
            err = network_mysqld_proto_get_lenenc_gstring(packet, auth->response);
            if (err != 0) {
                msg = "get auth response failed with CLIENT_SECURE_CONNECTION";
                goto fun_exit;
            }
        } else {
            err = network_mysqld_proto_get_gstring(packet, auth->response);
            if (err != 0) {
                msg = "get auth response failed without CLIENT_SECURE_CONNECTION";
                goto fun_exit;
            }
        }

        if (packet->offset != packet->data->len) {
            /* database is optional and may include a trailing \0 char */
            err = network_mysqld_proto_get_gstring_len(packet, packet->data->len - packet->offset, auth->database);
            if (err != 0) {
                msg = "get auth response's dbname failed";
                goto fun_exit;
            }
            if (auth->database->len > 0 && 
                (auth->database->str[auth->database->len - 1] == '\0')) {
                auth->database->len--;
            }
        }
    } else {
        err = network_mysqld_proto_get_int16(packet, &l_cap);
        if (err != 0) {
            msg = "get auth response's client capabilities failed before CLIENT_PROTOCOL_41";
            goto fun_exit;
        }
        err = network_mysqld_proto_get_int24(packet, &auth->max_packet_size);
        if (err != 0) {
            msg = "get auth response's max_packets_size failed before CLIENT_PROTOCOL_41";
            goto fun_exit;
        }
        err = network_mysqld_proto_get_gstring(packet, auth->username);
        if (err != 0) {
            msg = "get auth response's username failed before CLIENT_PROTOCOL_41";
            goto fun_exit;
        }
        /* there may be no password sent */
        if (packet->data->len != packet->offset) {
            err = network_mysqld_proto_get_gstring(packet, auth->response);
            if (err != 0) {
                msg = "get auth response failed before CLIENT_PROTOCOL_41";
                goto fun_exit;
        }
        }
        if (err == 0) {
            auth->capabilities = l_cap;
        }
    }

fun_exit:

    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}

/**
 * append the auth struct to the mysqld packet
 */
int network_mysqld_proto_append_auth_response(GString *packet, network_mysqld_auth_response *auth) {
    int i;

    if (!(auth->capabilities & CLIENT_PROTOCOL_41)) {
        network_mysqld_proto_append_int16(packet, auth->capabilities);
        network_mysqld_proto_append_int24(packet, auth->max_packet_size); /* max-allowed-packet */

        if (auth->username->len) g_string_append_len(packet, S(auth->username));
        network_mysqld_proto_append_int8(packet, 0x00); /* trailing \0 */

        if (auth->response->len) {
            g_string_append_len(packet, S(auth->response));
            network_mysqld_proto_append_int8(packet, 0x00); /* trailing \0 */
        }
    } else {
        network_mysqld_proto_append_int32(packet, auth->capabilities);
        network_mysqld_proto_append_int32(packet, auth->max_packet_size); /* max-allowed-packet */
        
        network_mysqld_proto_append_int8(packet, auth->charset); /* charset */

        for (i = 0; i < 23; i++) { /* filler */
            network_mysqld_proto_append_int8(packet, 0x00);
        }

        if (auth->username->len) g_string_append_len(packet, S(auth->username));
        network_mysqld_proto_append_int8(packet, 0x00); /* trailing \0 */

        /* scrambled password */
        network_mysqld_proto_append_lenenc_string_len(packet, S(auth->response));
        if (auth->database->len) {
            g_string_append_len(packet, S(auth->database));
            network_mysqld_proto_append_int8(packet, 0x00); /* trailing \0 */
        }
    }

    return 0;
}


network_mysqld_auth_response *network_mysqld_auth_response_copy(network_mysqld_auth_response *src) {
    network_mysqld_auth_response *dst;

    if (!src) return NULL;

    dst = network_mysqld_auth_response_new();
    dst->capabilities    = src->capabilities;
    dst->max_packet_size = src->max_packet_size;
    dst->charset         = src->charset;
    g_string_assign_len(dst->username, S(src->username));
    g_string_assign_len(dst->response, S(src->response));
    g_string_assign_len(dst->database, S(src->database));

    return dst;
}

/*
 * prepared statements
 */

/**
 * 
 */
network_mysqld_stmt_prepare_packet_t *network_mysqld_stmt_prepare_packet_new() {
    network_mysqld_stmt_prepare_packet_t *stmt_prepare_packet;

    stmt_prepare_packet = g_slice_new0(network_mysqld_stmt_prepare_packet_t);
    stmt_prepare_packet->stmt_text = g_string_new(NULL);

    return stmt_prepare_packet;
}

/**
 * 
 */
void network_mysqld_stmt_prepare_packet_free(network_mysqld_stmt_prepare_packet_t *stmt_prepare_packet) {
    if (NULL == stmt_prepare_packet) return;

    if (NULL != stmt_prepare_packet->stmt_text) g_string_free(stmt_prepare_packet->stmt_text, TRUE);

    g_slice_free(network_mysqld_stmt_prepare_packet_t, stmt_prepare_packet);
}

/**
 * 
 */
int network_mysqld_proto_get_stmt_prepare_packet(network_packet *packet, network_mysqld_stmt_prepare_packet_t *stmt_prepare_packet) {
    guint8 packet_type;

    int err = 0;

    err = network_mysqld_proto_get_int8(packet, &packet_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get packet_type failed");
        return -1;
    }

    if (COM_STMT_PREPARE != packet_type) {
        gchar *msg = g_strdup_printf("expected the first byte to be %02x, got %02x",
                                    COM_STMT_PREPARE, packet_type);
        EMIT_SIMPLE_CRITICAL(msg);
        g_free(msg);
        return -1;
    }

    g_string_assign_len(stmt_prepare_packet->stmt_text, packet->data->str + packet->offset, packet->data->len - packet->offset);

    return 0;
}

int network_mysqld_proto_append_stmt_prepare_packet(GString *packet, network_mysqld_stmt_prepare_packet_t *stmt_prepare_packet) {
    network_mysqld_proto_append_int8(packet, COM_STMT_PREPARE);
    g_string_append_len(packet, S(stmt_prepare_packet->stmt_text));

    return 0;
}

/**
 * 
 */
network_mysqld_stmt_prepare_ok_packet_t *network_mysqld_stmt_prepare_ok_packet_new() {
    network_mysqld_stmt_prepare_ok_packet_t *stmt_prepare_ok_packet;

    stmt_prepare_ok_packet = g_slice_new0(network_mysqld_stmt_prepare_ok_packet_t);

    return stmt_prepare_ok_packet;
}

/**
 * 
 */
void network_mysqld_stmt_prepare_ok_packet_free(network_mysqld_stmt_prepare_ok_packet_t *stmt_prepare_ok_packet) {
    if (NULL == stmt_prepare_ok_packet) return;

    g_slice_free(network_mysqld_stmt_prepare_ok_packet_t, stmt_prepare_ok_packet);
}

/**
 * parse the first packet of the OK response for a COM_STMT_PREPARE
 *
 * it is followed by the field defs for the params and the columns and their EOF packets which is handled elsewhere
 */
int network_mysqld_proto_get_stmt_prepare_ok_packet(network_packet *packet, network_mysqld_stmt_prepare_ok_packet_t *stmt_prepare_ok_packet) {
    guint8 packet_type;
    guint16 num_columns;
    guint16 num_params;
    guint16 warnings;
    guint32 stmt_id;
    gchar *msg = NULL;

    int err = 0;

    err = network_mysqld_proto_get_int8(packet, &packet_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get packet_type failed");
        return -1;
    }

    if (packet_type != 0x00) {
        gchar *msg = g_strdup_printf("expected the first byte to be %02x, got %02x",
                                                        0x00, packet_type);
        EMIT_SIMPLE_CRITICAL(msg);
        g_free(msg);
        return -1;
    }
    err = network_mysqld_proto_get_int32(packet, &stmt_id);
    if (err != 0) {
        msg = "get stmt_id failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int16(packet, &num_columns);
    if (err != 0) {
        msg = "get columns number failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int16(packet, &num_params);
    if (err != 0) {
        msg = "get parameters number failed";
        goto funcexit;
    }
    err = network_mysqld_proto_skip(packet, 1); /* the filler */
    if (err != 0) {
        msg = "get 1-byte placeholder failed";
        goto funcexit;
    }
    err = network_mysqld_proto_get_int16(packet, &warnings);
    if (err != 0) {
        msg = "get warnings failed";
        goto funcexit;
    }

    if (err == 0) {
        stmt_prepare_ok_packet->stmt_id = stmt_id;
        stmt_prepare_ok_packet->num_columns = num_columns;
        stmt_prepare_ok_packet->num_params = num_params;
        stmt_prepare_ok_packet->warnings = warnings;
    }

funcexit:
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL(msg);
    }

    return err ? -1 : 0;
}

int network_mysqld_proto_append_stmt_prepare_ok_packet(GString *packet, network_mysqld_stmt_prepare_ok_packet_t *stmt_prepare_ok_packet) {
    int err = 0;

    err = err || network_mysqld_proto_append_int8(packet, MYSQLD_PACKET_OK);
    err = err || network_mysqld_proto_append_int32(packet, stmt_prepare_ok_packet->stmt_id);
    err = err || network_mysqld_proto_append_int16(packet, stmt_prepare_ok_packet->num_columns);
    err = err || network_mysqld_proto_append_int16(packet, stmt_prepare_ok_packet->num_params);
    err = err || network_mysqld_proto_append_int8(packet, 0x00);
    err = err || network_mysqld_proto_append_int16(packet, stmt_prepare_ok_packet->warnings);

    return err ? -1 : 0;
}

/**
 * create a struct for a COM_STMT_EXECUTE packet
 */
network_mysqld_stmt_execute_packet_t *network_mysqld_stmt_execute_packet_new() {
    network_mysqld_stmt_execute_packet_t *stmt_execute_packet;

    stmt_execute_packet = g_slice_new0(network_mysqld_stmt_execute_packet_t);
    stmt_execute_packet->params = g_ptr_array_new();

    return stmt_execute_packet;
}

/**
 * free a struct for a COM_STMT_EXECUTE packet
 */
void network_mysqld_stmt_execute_packet_free(network_mysqld_stmt_execute_packet_t *stmt_execute_packet) {
    guint i;

    if (NULL == stmt_execute_packet) return;

    for (i = 0; i < stmt_execute_packet->params->len; i++) {
        network_mysqld_type_t *param = g_ptr_array_index(stmt_execute_packet->params, i);

        network_mysqld_type_free(param);
    }

    g_ptr_array_free(stmt_execute_packet->params, TRUE);

    g_slice_free(network_mysqld_stmt_execute_packet_t, stmt_execute_packet);
}

/**
 * get the statement-id from the COM_STMT_EXECUTE packet
 *
 * as network_mysqld_proto_get_stmt_execute_packet() needs the parameter count
 * to calculate the number of null-bits, we need a way to look it up in a 
 * external store which is very likely indexed by the stmt-id
 *
 * @see network_mysqld_proto_get_stmt_execute_packet()
 */
int network_mysqld_proto_get_stmt_execute_packet_stmt_id(network_packet *packet,
        guint32 *stmt_id) {
    guint8 packet_type;
    int err = 0;

    err = network_mysqld_proto_get_int8(packet, &packet_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get packet_type failed");
        return -1;
    }

    if (COM_STMT_EXECUTE != packet_type) {
        gchar *msg = g_strdup_printf("expected the first byte to be %02x, got %02x",
                                    COM_STMT_EXECUTE, packet_type);
        EMIT_SIMPLE_CRITICAL(msg);
        g_free(msg);
        return -1;
    }

    err = network_mysqld_proto_get_int32(packet, stmt_id);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get stmt_id failed");
        return -1;
    }
    return 0;
}

/**
 *
 * param_count has to be taken from the response of the prepare-stmt-ok packet
 *
 * @param param_count number of parameters that we expect to see here
 */
int network_mysqld_proto_get_stmt_execute_packet(network_packet *packet,
        network_mysqld_stmt_execute_packet_t *stmt_execute_packet,
        guint param_count) {
    int err = 0;
    GString *nul_bits;
    gsize nul_bits_len;

    err = network_mysqld_proto_get_stmt_execute_packet_stmt_id(packet, &stmt_execute_packet->stmt_id);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get execute stmt_id failed");
        return -1;
    }

    err = network_mysqld_proto_get_int8(packet, &stmt_execute_packet->flags);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get execute stmt flags failed");
        return -1;
    }
    err = network_mysqld_proto_get_int32(packet, &stmt_execute_packet->iteration_count);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get 4-bytes reserved number failed");
        return -1;
    }

    if (0 == param_count) {
        return err ? -1 : 0;
    }

    nul_bits_len = (param_count + 7) / 8;
    nul_bits = g_string_sized_new(nul_bits_len);
    err = network_mysqld_proto_get_gstring_len(packet, nul_bits_len, nul_bits);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get null-bitmaps failed");
        return -1;
    }
    err = network_mysqld_proto_get_int8(packet, &stmt_execute_packet->new_params_bound);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get 1-bytes params split flag failed");
        return -1;
    }

    if (0 != err) {
        g_string_free(nul_bits, TRUE);

        return -1; /* exit early if something failed up to now */
    }

    if (stmt_execute_packet->new_params_bound) {
        guint i;

        for (i = 0; 0 == err && i < param_count; i++) {
            guint16 param_type;

            err = network_mysqld_proto_get_int16(packet, &param_type);
            if (err != 0) {
                gchar *err_msg = g_strdup_printf("get %dth param type failed", i);
                EMIT_SIMPLE_CRITICAL(err_msg);
                g_free(err_msg);
            }
            if (err == 0) {
                network_mysqld_type_t *param;

                param = network_mysqld_type_new(param_type & 0xff);
                if (NULL == param) {
                    g_log_dbproxy(g_warning, "couldn't create type = %d", param_type & 0xff);

                    err = -1;
                    break;
                }
                param->is_null = (nul_bits->str[i / 8] & (1 << (i % 8))) != 0;
                param->is_unsigned = (param_type & 0x8000) != 0;

                g_ptr_array_add(stmt_execute_packet->params, param);
            }
        }

        for (i = 0; 0 == err && i < param_count; i++) {
            network_mysqld_type_t *param = g_ptr_array_index(stmt_execute_packet->params, i);

            if (!param->is_null) {
                err = network_mysqld_proto_binary_get_type(packet, param);
                if (err != 0) {
                    gchar *err_msg = g_strdup_printf("get %dth param binary type failed", i);
                    EMIT_SIMPLE_CRITICAL(err_msg);
                    g_free(err_msg);
                }
            }
        }
    }

    g_string_free(nul_bits, TRUE);

    return err ? -1 : 0;
}

int network_mysqld_proto_append_stmt_execute_packet(GString *packet,
        network_mysqld_stmt_execute_packet_t *stmt_execute_packet,
        guint param_count) {
    gsize nul_bits_len;
    GString *nul_bits;
    guint i;
    int err = 0;

    nul_bits_len = (param_count + 7) / 8;
    nul_bits = g_string_sized_new(nul_bits_len);
    memset(nul_bits->str, 0, nul_bits->len); /* set it all to zero */

    for (i = 0; i < param_count; i++) {
        network_mysqld_type_t *param = g_ptr_array_index(stmt_execute_packet->params, i);

        if (param->is_null) {
            nul_bits->str[i / 8] |= 1 << (i % 8);
        }
    }

    network_mysqld_proto_append_int8(packet, COM_STMT_EXECUTE);
    network_mysqld_proto_append_int32(packet, stmt_execute_packet->stmt_id);
    network_mysqld_proto_append_int8(packet, stmt_execute_packet->flags);
    network_mysqld_proto_append_int32(packet, stmt_execute_packet->iteration_count);
    g_string_append_len(packet, S(nul_bits));
    network_mysqld_proto_append_int8(packet, stmt_execute_packet->new_params_bound);

    if (stmt_execute_packet->new_params_bound) {
        for (i = 0; i < stmt_execute_packet->params->len; i++) {
            network_mysqld_type_t *param = g_ptr_array_index(stmt_execute_packet->params, i);

            network_mysqld_proto_append_int16(packet, (guint16)param->type);
        }
        for (i = 0; 0 == err && i < stmt_execute_packet->params->len; i++) {
            network_mysqld_type_t *param = g_ptr_array_index(stmt_execute_packet->params, i);
            
            if (!param->is_null) {
                err = err || network_mysqld_proto_binary_append_type(packet, param);
            }
        }
    }

    return err ? -1 : 0;
}

/**
 * create a struct for a COM_STMT_EXECUTE resultset row
 */
network_mysqld_resultset_row_t *network_mysqld_resultset_row_new() {
    return g_ptr_array_new();
}

/**
 * free a struct for a COM_STMT_EXECUTE resultset row
 */
void network_mysqld_resultset_row_free(network_mysqld_resultset_row_t *row) {
    guint i;

    if (NULL == row) return;

    for (i = 0; i < row->len; i++) {
        network_mysqld_type_t *field = g_ptr_array_index(row, i);

        network_mysqld_type_free(field);
    }

    g_ptr_array_free(row, TRUE);
}

/**
 * get the fields of a row that is in binary row format
 */
int network_mysqld_proto_get_binary_row(network_packet *packet, network_mysqld_proto_fielddefs_t *coldefs, network_mysqld_resultset_row_t *row) {
    int err = 0;
    guint i;
    guint nul_bytes_len;
    GString *nul_bytes;
    guint8 ok;

    err = network_mysqld_proto_get_int8(packet, &ok); /* the packet header which seems to be always 0 */
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get ok flags failed");
        return -1;
    }
    err = err || (ok != 0);

    nul_bytes_len = (coldefs->len + 7 + 2) / 8; /* the first 2 bits are reserved */
    nul_bytes = g_string_sized_new(nul_bytes_len);
    err = network_mysqld_proto_get_gstring_len(packet, nul_bytes_len, nul_bytes);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get null-bytes failed");
        return -1;
    }

    for (i = 0; 0 == err && i < coldefs->len; i++) {
        network_mysqld_type_t *param;
        network_mysqld_proto_fielddef_t *coldef = g_ptr_array_index(coldefs, i);

        param = network_mysqld_type_new(coldef->type);
        if (NULL == param) {
            g_log_dbproxy(g_warning, "couldn't create type = %d", coldef->type);
            err = -1;
            break;
        }

        if (nul_bytes->str[(i + 2) / 8] & (1 << ((i + 2) % 8))) {
            param->is_null = TRUE;
        } else {
            err = network_mysqld_proto_binary_get_type(packet, param);
            if (err != 0) {
                gchar *err_msg = g_strdup_printf("get param type failed");
                EMIT_SIMPLE_CRITICAL(err_msg);
                g_free(err_msg);
            }
        }

        g_ptr_array_add(row, param);
    }

    g_string_free(nul_bytes, TRUE);

    return err ? -1 : 0;
}

/**
 */
GList *network_mysqld_proto_get_next_binary_row(GList *chunk, network_mysqld_proto_fielddefs_t *fields, network_mysqld_resultset_row_t *row) {
    network_packet packet;
    int err = 0;
    network_mysqld_lenenc_type lenenc_type;
    
    packet.data = chunk->data;
    packet.offset = 0;

    err = network_mysqld_proto_skip_network_header(&packet);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get network header failed");
        return NULL;
    }

    err = network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get lenenc type failed");
        return NULL;
    }

    if (0 != err) return NULL;

    if (NETWORK_MYSQLD_LENENC_TYPE_EOF == lenenc_type) {
        /* this is a EOF packet, we are done */
        return NULL;
    }

    err = network_mysqld_proto_get_binary_row(&packet, fields, row);

    return ((err != 0) ? NULL : chunk->next);
}

/**
 * create a struct for a COM_STMT_CLOSE packet
 */
network_mysqld_stmt_close_packet_t *network_mysqld_stmt_close_packet_new() {
    network_mysqld_stmt_close_packet_t *stmt_close_packet;

    stmt_close_packet = g_slice_new0(network_mysqld_stmt_close_packet_t);

    return stmt_close_packet;
}

/**
 * free a struct for a COM_STMT_CLOSE packet
 */
void network_mysqld_stmt_close_packet_free(network_mysqld_stmt_close_packet_t *stmt_close_packet) {
    if (NULL == stmt_close_packet) return;

    g_slice_free(network_mysqld_stmt_close_packet_t, stmt_close_packet);
}

/**
 */
int network_mysqld_proto_get_stmt_close_packet(network_packet *packet, network_mysqld_stmt_close_packet_t *stmt_close_packet) {
    guint8 packet_type;
    int err = 0;

    err = network_mysqld_proto_get_int8(packet, &packet_type);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get packet type failed");
        return -1;
    }

    if (COM_STMT_CLOSE != packet_type) {
        gchar *msg = g_strdup_printf("expected the first byte to be %02x, got %02x",
                                                        COM_STMT_CLOSE, packet_type);
        EMIT_SIMPLE_CRITICAL(msg);
        g_free(msg);
        return -1;
    }

    err = network_mysqld_proto_get_int32(packet, &stmt_close_packet->stmt_id);
    if (err != 0) {
        EMIT_SIMPLE_CRITICAL("get stmt_id failed");
        return -1;
    }

    return 0;
}

int network_mysqld_proto_append_stmt_close_packet(GString *packet, network_mysqld_stmt_close_packet_t *stmt_close_packet) {
    network_mysqld_proto_append_int8(packet, COM_STMT_CLOSE);
    network_mysqld_proto_append_int32(packet, stmt_close_packet->stmt_id);

    return 0;
}

GString* network_mysqld_auth_challenge_dump(network_mysqld_auth_challenge *shake) {
    GString *challenge_dump = g_string_new(NULL);
    
    g_string_append_printf(challenge_dump,
                            "protocol_version:%d server_version_str:%s server_version:%d "
                            "thread_id:%d capabilities:%d "
                            "charset:%d server_status:%d challenge:%s",
                             shake->protocol_version, shake->server_version_str, shake->server_version,
                             shake->thread_id, shake->capabilities,
                             shake->charset, shake->server_status, shake->challenge->str);

    return challenge_dump;
}

GString* network_mysqld_auth_response_dump(network_mysqld_auth_response *auth) {
    GString *response_dump = g_string_new(NULL);

    g_string_append_printf(response_dump,
                           "capabilities:%d max_packet_size:%d charset:%d "
                           "username:%s database:%s response:%x",
                            auth->capabilities, auth->max_packet_size, auth->charset,
                            auth->username->str, auth->database->str, auth->response->str);
    return response_dump;
}
