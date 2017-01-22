--[[ $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ --]]

function set_error(errmsg, nologerr)
    if nologerr then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            nologerr = nologerr,
            errmsg = errmsg or "error"
        }
    else
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            errmsg = errmsg or "error"
        }   
    end
end

function read_query(packet)
    if packet:byte() ~= proxy.COM_QUERY then
        set_error("[admin] we only handle text-based queries (COM_QUERY)", 1)
        return proxy.PROXY_SEND_RESULT
    end

    local query = packet:sub(2)

    local rows = { }
    local fields = { }

    if string.find(query:lower(), "^select%s+*%s+from%s+backends$") then
        fields = {
            { name = "backend_ndx", 
              type = proxy.MYSQL_TYPE_LONG },
            { name = "address",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "hostname",
                          type = proxy.MYSQL_TYPE_STRING },
            { name = "state",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "type",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "weight",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "tag",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "threads_running",
              type = proxy.MYSQL_TYPE_STRING },
        }

        for i = 1, #proxy.global.backends do
            local states = {
                "unknown",
                "up",
                "pending",
                "down",
                "offlining",
                "offline",
                "removing"
                
            }
            local types = {
                "unknown",
                "rw",
                "ro"
            }
            local b = proxy.global.backends[i]

            rows[#rows + 1] = {
                i,
                b.dst.name,          -- configured backend address
                b.hostname,          -- show the host name
                states[b.state + 1], -- the C-id is pushed down starting at 0
                types[b.type + 1],   -- the C-id is pushed down starting at 0
                b.weight,   -- the C-id is pushed down starting at 0
                b.tag,   -- the C-id is pushed down starting at 0
                b.threads_running,
            }
        end
    elseif string.find(query:lower(), "^set%s+%a+%s+%d+.?") then
            local state
            local id
            local timeout
            local ret = 0
    
            if string.find(query:lower(), "^set%s+%a+%s+%d+$") then
                state,id = string.match(query:lower(), "^set%s+(%a+)%s+(%d+)$")
                timeout = 0
            elseif string.find(query:lower(), "^set%s+%a+%s+%d+%s+timeout%s+%d+$") then
                state, id, timeout = string.match(query:lower(), "^set%s+(%a+)%s+(%d+)%s+timeout%s+(%d+)$")
            else
                set_error("invalid operation")
                return proxy.PROXY_SEND_RESULT
            end

        if proxy.global.backends[id] == nil then
            set_error("backend id is not exsit")
            return proxy.PROXY_SEND_RESULT
        end

        if state == "offline" then
            ret = proxy.global.backends(proxy.BACKEND_STATE_OFFLINING, id - 1, timeout, proxy.BACKEND_STATE);
        elseif state == "online" then
            ret = proxy.global.backends(proxy.BACKEND_STATE_UNKNOWN, id - 1, timeout, proxy.BACKEND_STATE);
        else
            set_error("invalid operation")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status", 
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^add%s+master%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?$") then
            local newserver = string.match(query:lower(), "^add%s+master%s+(.+)$")
            proxy.global.backends.addmaster = newserver
        if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

        fields = {
            { name = "status", 
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^add%s+slave%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?.*$") then
            local newserver = string.match(query:lower(), "^add%s+slave%s+(.+)$")
            proxy.global.backends.addslave = newserver
        if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

        fields = {
            { name = "status", 
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^remove%s+backend%s+%d+.?") then
            local newserver
            local timeout

            if string.find(query:lower(), "^remove%s+backend%s+%d+$") then
                newserver = string.match(query:lower(), "^remove%s+backend%s+(%d+)$")
                timeout = 0
            elseif string.find(query:lower(), "^remove%s+backend%s+%d+%s+timeout%s+%d+$") then
                newserver, timeout = string.match(query:lower(), "remove%s+backend%s+(%d+)%s+timeout%s+(%d+)$")
            end
            newserver = tonumber(newserver)
            if newserver <= 0 or newserver > #proxy.global.backends then
                set_error("invalid backend_id")
                return proxy.PROXY_SEND_RESULT
            else
                local ret = proxy.global.backends(proxy.BACKEND_STATE_REMOVING, newserver - 1, timeout, proxy.BACKEND_STATE)
                if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

                if ret == 1 then
                    set_error("remove backend failed")
                    return proxy.PROXY_SEND_RESULT
                end

            fields = {
                { name = "status", 
                  type = proxy.MYSQL_TYPE_STRING },
            }
        end
    elseif string.find(query:lower(), "^select%s+*%s+from%s+clients$") then
        fields = {
            { name = "client",
              type = proxy.MYSQL_TYPE_STRING },
        }
        for i = 1, #proxy.global.clients do
            rows[#rows + 1] = {
                proxy.global.clients[i]
            }
        end
    elseif string.find(query:lower(), "^add%s+client%s+(.+)$") then
        local client = string.match(query:lower(), "^add%s+client%s+(.+)$")

        if proxy.global.clients(client) == 1 then
            set_error("this client is exist")
            return proxy.PROXY_SEND_RESULT
        end

        proxy.global.backends.addclient = client
        fields = {
                  { name = "status",
                    type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^remove%s+client%s+(.+)$") then
        local client = string.match(query:lower(), "^remove%s+client%s+(.+)$")

        if proxy.global.clients(client) == 0 then
            set_error("this client is NOT exist")
            return proxy.PROXY_SEND_RESULT
        end

        proxy.global.backends.removeclient = client
        fields = {
                  { name = "status",
                    type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^select%s+*%s+from%s+pwds$") then
        local users_info = proxy.global.pwds.pwds
        fields = {
            { name = "username",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "password",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "hosts",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "backends",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "type",
                          type = proxy.MYSQL_TYPE_STRING }
        }
        
        if nil ~= users_info then
                    for k, v in pairs(users_info) do
                         rows[#rows + 1] = {v["username"], v["password"], v["hosts"], v["backends"], v["type"]}
                    end
        end
    elseif string.find(query:lower(), "^%s*show%s+tables%s*") then
        local table_custom = ""
        local tables

        if string.find(query:lower(), "^%s*show%s+tables%s+like%s+'.*'%s*$") then
            table_custom = string.match(query, "^%s*[sS][hH][oO][wW]%s+[tT][aA][bB][lL][eE][sS]%s+[lL][iI][kK][eE]%s+'(.*)'%s*$");
        end

        tables = proxy.global.sys_config(table_custom, "show_tables")
        local key_table = {}

        fields = {
            { name = "db",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "tables",
              type = proxy.MYSQL_TYPE_STRING }
        }

        if nil ~= tables then
            local key_table = {}

            for k,_ in pairs(tables) do
                table.insert(key_table, k)
            end

            table.sort(key_table)

            for _,k in pairs(key_table) do
                rows[#rows + 1] = {tables[k]["db"], k}
            end
        end
    elseif string.find(query:lower(), "^%s*remove%s+tables%s+like%s+'.*'%s*$") then
        local tables = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[tT][aA][bB][lL][eE][sS]%s+[l][iI][kK][eE]%s+'(.*)'%s*$");
        local ret = proxy.global.sys_config(tables, "remove_tables")

        if ret == 1 then
            set_error("invalid tables" .. tables)
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("invalid option" .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*add%s+tables%s+'.*'%s*$") then
        local tables = string.match(query, "^%s*[aA][dD][dD]%s+[tT][aA][bB][lL][eE][sS]%s+'(.*)'%s*$")
        if tables == nil or string.len(tables) == 0 then
            set_error("invalid tables")
            return proxy.PROXY_SEND_RESULT
        end

        local ret = proxy.global.sys_config(tables, "add_tables")

        if ret == 1 then
            set_error("invalid tables " .. tables)
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("invalid option" .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[aA][dD][dD]%s+[pP][wW][dD]%s+(.+):(.+)$") then
        local user, pwd = string.match(query, "^[aA][dD][dD]%s+[pP][wW][dD]%s+(.+):(.+)$")
        local ret = proxy.global.backends(user, pwd, proxy.ADD_PWD)

        if ret == 1 then
            set_error("this user is exist")
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("failed to encrypt")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[aA][dD][dD]%s+[eE][nN][pP][wW][dD]%s+(.+):(.+)$") then
        local user, pwd = string.match(query, "^[aA][dD][dD]%s+[eE][nN][pP][wW][dD]%s+(.+):(.+)$")
        local ret = proxy.global.backends(user, pwd, proxy.ADD_ENPWD)

        if ret == 1 then
            set_error("this user is exist")
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("failed to decrypt")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[rR][eE][mM][oO][vV][eE]%s+[pP][wW][dD]%s+(.+)$") then
        local user = string.match(query, "^[rR][eE][mM][oO][vV][eE]%s+[pP][wW][dD]%s+(.+)$")
        local ret = proxy.global.backends(user, nil, proxy.REMOVE_PWD)

        if ret == 1 then
            set_error("this user is NOT exist")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^save%s+config+$") then
            local ret = proxy.global.sys_config("", "saveconfig")

            if ret == 1 then
                set_error("open config-file failed")
                return proxy.PROXY_SEND_RESULT
             elseif ret == 2 then
                set_error("write config-file failed")
                return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^select%s+version+$") then
        fields = {
            { name = "version",
              type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { proxy.PROXY_VERSION }
    elseif string.find(query:lower(), "^show%s+proxy%s+status+$") then
        local status_vars = proxy.global.status.proxy_status
        
        fields = {
            { name = "Variable_name",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Value",
              type = proxy.MYSQL_TYPE_STRING },
        }
        
        if nil ~= status_vars then 
            for k, v in pairs(status_vars) do
                if k == "Query_filter_frequent_threshold" then
                    local v_ceil
                    v = v * 1000
                    v_ceil = math.ceil(v)
                    if v_ceil - v > 0.5 then
                        v = math.floor(v)
                        v = v / 1000
                    else
                        v = v_ceil / 1000
                    end
                end
                rows[#rows + 1] = {k, v}
            end
        end
    elseif string.find(query:lower(), "^show%s+events%s+waits%s+status+$") then
        local event_stat = proxy.global.status.wait_event_stat
        
        fields = {
            { name = "event_name",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "waits",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "slow_waits",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "min_wait_time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "max_wait_time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "total_wait_time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "total_slow_wait_time",
              type = proxy.MYSQL_TYPE_STRING },
        }
        
        if nil ~= event_stat then 
            for k, v in pairs(event_stat) do
                rows[#rows + 1] = {v[0], v[1], v[2], v[3], v[4], v[5], v[6]}
            end
        end     
    elseif string.find(query:lower(), "^show%s+processlist+$") then
        local connections = proxy.global.status.processlist
        fields = {
            { name = "Id",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "User",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Event Thread Id",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Host",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "db",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "backend",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "thread_id",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Info",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "State",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Waiting",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Auto Commit",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "In Transaction",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "DB Connected",
              type = proxy.MYSQL_TYPE_STRING },
        }
        
        if nil ~= connections then      
            for k, v in pairs(connections) do
                rows[#rows + 1] = {k, v["user"], v["event_thread"], v["host"], v["db"], v["backend"], v["thread_id"], v["time"], v["info"], v["state"], v["wait_status"], v["auto_commit"], v["in_trx"], v["db_connected"]}
            end
        end
    elseif string.find(query:lower(), "^show%s+query_response_time+$") then
        local query_response_times = proxy.global.status.query_response_time
        fields = {
            { name = "time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "type",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "count",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "total",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "min_time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "max_time",
              type = proxy.MYSQL_TYPE_STRING }
        }

        for i=0, #query_response_times do
            v = query_response_times[i]
            rows[#rows + 1] = {v[0], v[1], v[2], v[3], v[4], v[5]}
        end
    elseif string.find(query:lower(), "^%s*show%s+variables") then
        local value = ""
        local vars

        if string.find(query:lower(), "^%s*show%s+variables%s+like%s+'%S+'%s*$") then
            value = string.match(query:lower(), "^%s*show%s+variables%s+like%s+'(%S+)'%s*$")
        end

        vars = proxy.global.sys_config(value, "show_variables")

        local key_table = {}

        fields = {
            { name = "Variable_name",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Group",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Value",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "Set mode",
              type = proxy.MYSQL_TYPE_STRING }
        }

        if nil ~= vars then
            for key,_ in pairs(vars) do
                table.insert(key_table, key)
            end

            table.sort(key_table)

            for _, key in pairs(key_table) do
                rows[#rows + 1] = {key, vars[key]["group"], vars[key]["value"], vars[key]["set_mode"]}
            end
        end
    elseif string.find(query:lower(), "^%s*show%s+lastest_queries%s*$") then
        local lastest_queries
        if string.find(query:lower(), "^%s*show%s+lastest_queries%s*$") then
            lastest_queries = proxy.global.status.lastest_queries
        end

        fields = {
            { name = "query_rewrite",
              type = proxy.MYSQL_TYPE_STRING }, 
            { name = "fist_access_time",
              type = proxy.MYSQL_TYPE_STRING }, 
            { name = "last_access_time",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "query_times",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "lastest_query_times",
              type = proxy.MYSQL_TYPE_STRING }
            }

        if nil ~= lastest_queries then
            for k, v in pairs(lastest_queries) do
                rows[#rows + 1] = {v["query_rewrite"],
                                   v["fist_access_time"], v["last_access_time"],
                                   v["query_times"], v["lastest_query_times"]}
            end
        end
    elseif string.find(query:lower(), "^%s*show%s+blacklists%s*$") then
        local blacklists = proxy.global.status.blacklists
        fields = { 
            { name = "filter_hashcode",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "filter",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "is_enabled",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "filter_status",
              type = proxy.MYSQL_TYPE_STRING },
            { name = "hit_times",
              type = proxy.MYSQL_TYPE_STRING }
        }

        if nil ~= blacklists then
            for k, v in pairs(blacklists) do
                rows[#rows + 1] = {v["filter_hashcode"], v["filter"],
                                   v["is_enabled"], v["filter_status"], v["hit_times"]}
            end
        end
    elseif string.find(query, "^%s*[aA][dD][dD]%s+[bB][lL][aA][cC][kK][lL][iI][sS][tT]%s+'.+'%s*%d*$") then
        local filter, flag = string.match(query, "^%s*[aA][dD][dD]%s+[bB][lL][aA][cC][kK][lL][iI][sS][tT]%s+'(.+)'%s*(%d*)$")

        local ret = proxy.global.sys_config(flag, filter, "addblacklist")

        if ret == 1 then
            set_error("invalid filter flag: " .. flag)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
   elseif string.find(query:lower(), "^%s*remove%s+blacklist%s+'%S+'%s*$") then
        local hash_code = string.match(query:lower(), "^%s*remove%s+blacklist%s+'(%S+)'%s*$")
        local ret = proxy.global.sys_config(hash_code, "removeblacklist")

        if ret == 1 then
            set_error("invalid filter hash code: " .. hash_code)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        } 
    elseif string.find(query:lower(), "^%s*set%s+blacklist%s+'.+'%s+%S+%s*$") then
        local hash_code, flag = string.match(query:lower(), "^%s*set%s+blacklist%s+'(.+)'%s+(%S+)%s*$")                      
        local ret = proxy.global.sys_config(flag, hash_code, "updateblacklist")

        if ret == 1 then
            set_error("invalid filter flag: " .. flag)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*clear%s+blacklists%s*$") then
        local ret = proxy.global.sys_config("", "clearblacklist")

        if ret == 2 then
            set_error("invalid operation: " .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*save%s+blacklists%s*$") then
        local ret = proxy.global.sys_config("", "saveblacklist")

        if ret == 1 then
            set_error("save blacklist file falied, please check the admin log to find reason. ")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*load%s+blacklists%s*$") then
        local ret = proxy.global.sys_config("", "loadblacklist") 
        
        if ret == 2 then
            set_error("invalid operation: " .. query)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*set%s+%S+%s*=%s*%S+$") then
        local option, value = string.match(query, "^%s*[sS][eE][tT]%s+(%S+)%s*=%s*(%S+)$")
        local ret = 0;

        if (option:lower() ~= "backend-monitor-pwds") then
            ret = proxy.global.sys_config(value:lower(), option:lower())
        else
            ret = proxy.global.sys_config(value, option:lower())
        end

        if ret == 1 then
            set_error("invalid operation value: " .. value)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. option)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*shutdown%s*%a*%s*$") then
        local value = string.match(query:lower(), "^%s*shutdown%s*(%a*)$")
        local ret = proxy.global.sys_config(value, "shutdown")

        if ret == 1 then
            set_error("invalid operation value: " .. value)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. option)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^%s*[aA][dD][dD]%s+[uU][sS][eE][rR]%s+[hH][oO][sS][tT][sS]%s+%S+@.*$") then
        local user, hosts = string.match(query, "^%s*[aA][dD][dD]%s+[uU][sS][eE][rR]%s+[hH][oO][sS][tT][sS]%s+(%S+)@(.*)$")
        local ret = proxy.global.backends(user, hosts, proxy.ADD_USER_HOST)

        if ret == 1 then
            set_error("invalid user: " .. user)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*add%s+admin%s+user%s+hosts%s+.*$") then
        local hosts = string.match(query:lower(), "^%s*add%s+admin%s+user%s+hosts%s+(.*)$")
        local ret = proxy.global.backends("", hosts, proxy.ADD_ADMIN_HOSTS)

        if ret == 1 then
            set_error("invalid operation value: " .. hosts)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. user)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING }
        }
    elseif string.find(query:lower(), "^%s*remove%s+admin%s+user%s+hosts%s+.*$") then
        hosts = string.match(query:lower(), "^%s*remove%s+admin%s+user%s+hosts%s+(.*)$")
        local ret = proxy.global.backends("", hosts, proxy.REMOVE_ADMIN_HOSTS)

        if ret == 1 then
            set_error("invalid operation value: " .. hosts)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: ")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[aA][lL][tT][eE][rR]%s+[aA][dD][mM][iI][nN]%s+[uU][sS][eE][rR]%s+(%S+):(%S+)$") then
        local user, pwd = string.match(query, "^[aA][lL][tT][eE][rR]%s+[aA][dD][mM][iI][nN]%s+[uU][sS][eE][rR]%s+(%S+):(%S+)$")
        local ret = proxy.global.backends(user, pwd, proxy.ALTTER_ADMIN_PWDS)

        if ret == 1 then
            set_error("username or password should not be blank character")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
              type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*remove%s+user%s+hosts%s+%S+.*$") then
        local user, hosts

        if string.find(query:lower(), "^%s*remove%s+user%s+hosts%s+%S+@.*$") then
            user, hosts = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[uU][sS][eE][rR]%s+[hH][oO][sS][tT][sS]%s+(%S+)@(.*)$")
        elseif string.find(query:lower(), "^%s*remove%s+user%s+hosts%s+%S+%s*$") then
            user = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[uU][sS][eE][rR]%s+[hH][oO][sS][tT][sS]%s+(%S+)%s*$")
            hosts = ""
        end
        local ret = proxy.global.backends(user, hosts, proxy.REMOVE_USER_HOST)

        if ret == 1 then
            set_error("invalid user: " .. user)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*add%s+user%s+backends%s+%S+@%S+%s*$") then
        local user, backends = string.match(query, "^%s*[aA][dD][dD]%s+[uU][sS][eE][rR]%s+[bB][aA][cC][kK][eE][nN][dD][sS]%s+(%S+)@(.*)$")
        local ret = proxy.global.backends(user, backends, proxy.ADD_BACKENDS)

        if ret == 1 then
            set_error("user: " .. user .. " doesn't exist")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*remove%s+user%s+backends%s+%S+.*$") then
        local user, bakends

        if string.find(query:lower(), "^%s*remove%s+user%s+backends%s+%S+@.*$") then
            user, bakends = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[uU][sS][eE][rR]%s+[bB][aA][cC][kK][eE][nN][dD][sS]%s+(%S+)@(.*)$")
        elseif string.find(query:lower(), "^%s*remove%s+user%s+backends%s+%S+%s*$") then
            user = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[uU][sS][eE][rR]%s+[bB][aA][cC][kK][eE][nN][dD][sS]%s+(%S+)%s*$")
            bakends = ""
        end
        local ret = proxy.global.backends(user, bakends, proxy.REMOVE_BACKENDS)

        if ret == 1 then
            set_error("user: " .. user .. " doesn't exist")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(),"^%s*add%s+slave%s+tag%s+%S+%s+%S+%s*$") then 
        local tagname, backendindxs = string.match(query, "^%s*[aA][dD][dD]%s+[sS][lL][aA][vV][eE]%s+[tT][aA][gG]%s+(%S+)%s+(%S+)%s*$")
        proxy.global.backends.addslavetag = tagname..":"..backendindxs;
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*remove%s+slave%s+tag%s+%S+.*$")  then
        local tagname, backendindxs
        if string.find(query:lower(), "^%s*remove%s+slave%s+tag%s+%S+%s+%S+%s*$") then
            tagname, backendindxs  = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[sS][lL][aA][vV][eE]%s+[tT][aA][gG]%s+(%S+)%s+(%S+)%s*$")
        elseif string.find(query:lower(), "^%s*remove%s+slave%s+tag%s+%S+%s*$") then
            tagname = string.match(query, "^%s*[rR][eE][mM][oO][vV][eE]%s+[sS][lL][aA][vV][eE]%s+[tT][aA][gG]%s+(%S+)%s*$")
            backendindxs = ""
        else
            tagname = ""
            backendIndxs = ""
        end
        proxy.global.backends.removeslavetag = tagname..":"..backendindxs;

        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*alter%s+slave%s+weight%s+%d+%s+%d+%s*$") then
        local backendIndx, weight = string.match(query:lower(), "^%s*alter%s+slave%s+weight%s+(%d+)%s+(%d+)%s*$")   
        proxy.global.backends.alterweight = tostring(backendIndx-1)..":"..tostring(weight);
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
     elseif string.find(query:lower(), "^%s*kill%s*%a*%s+%w+%s*$") then
        local value = string.match(query:lower(), "^%s*kill%s*%a*%s+(%w+)%s*$")
        local ret = proxy.global.sys_config(value, "kill")

        if ret == 1 then
            set_error("invalid connection id: " .. value)
            return proxy.PROXY_SEND_RESULT
        elseif ret == 2 then
            set_error("invalid operation: " .. option)
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query:lower(), "^%s*show%s+percentile%s*%S*%s*$") then
    local rep_time = string.match(query:lower(), "^%s*show%s+percentile%s*(%S*)%s*$")
    if(0 >= string.len(rep_time)) then
        rep_time = "1m"
    end 
    local ret = proxy.global.sys_config(rep_time, "show_percentile")
    if tonumber(ret) == 1 then
        set_error("invalid operation value: " .. rep_time)
        return proxy.PROXY_SEND_RESULT
    end

    if tonumber(ret) == 2 then
        set_error("the percentile_switch is off")
        return proxy.PROXY_SEND_RESULT
    end

        fields = {
                { name = "percentile(μs)",
                type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows+1] = { ret };
    elseif string.find(query:lower(), "^select%s+*%s+from%s+help$") then
    fields = { 
        { name = "command", 
          type = proxy.MYSQL_TYPE_STRING },
        { name = "description", 
          type = proxy.MYSQL_TYPE_STRING },
    }
    rows[#rows + 1] = { "SELECT * FROM help", "shows this help" }

    rows[#rows + 1] = { "SELECT * FROM backends", "lists the backends and their state" }
    rows[#rows + 1] = { "SET OFFLINE $backend_id [timeout $int]", "offline backend server, $backend_id is backend_ndx's id, timeout in seconds" }
    rows[#rows + 1] = { "SET ONLINE $backend_id", "online backend server, ..." }
    rows[#rows + 1] = { "ADD MASTER $backend", "example: \"add master 127.0.0.1:3306\", ..." }
    rows[#rows + 1] = { "ADD SLAVE $backend", "example: \"add slave 127.0.0.1:3306$slave_tag\", ..." }
    rows[#rows + 1] = { "REMOVE BACKEND $backend_id [timeout $int]", "example: \"remove backend 1\",  timeout in seconds ..." }
    rows[#rows + 1] = { "SET remove-backend-timeout = $int", "online set the global timeout of remove/offline backend in seconds." }

    rows[#rows + 1] = { "SELECT * FROM clients", "lists the clients" }
    rows[#rows + 1] = { "ADD CLIENT $client", "example: \"add client 192.168.1.2\", ..." }
    rows[#rows + 1] = { "REMOVE CLIENT $client", "example: \"remove client 192.168.1.2\", ..." }

    rows[#rows + 1] = { "SELECT * FROM pwds", "lists the pwds and user host" }
    rows[#rows + 1] = { "ADD PWD $pwd", "example: \"add pwd user:raw_password\", ..." }
    rows[#rows + 1] = { "ADD ENPWD $pwd", "example: \"add enpwd user:encrypted_password\", ..." }
    rows[#rows + 1] = { "REMOVE PWD $pwd", "example: \"remove pwd user\", ..." }
    rows[#rows + 1] = { "ADD USER HOSTS $user_ips", "example: \"add user hosts usr@ip1|ip2\",  ..." }
    rows[#rows + 1] = { "REMOVE USER HOSTS $user_ips", "example: \"remove user hosts usr[@ip1|ip2]\",  ..." }
    rows[#rows + 1] = { "ADD USER BACKENDS $user_backends", "example: \"add user backends usr@lave_tag1[|slave_tag2]\",  ..." }
    rows[#rows + 1] = { "REMOVE USER BACKENDS $user_backends", "example: \"remove user backends usr[@lave_tag1[|slave_tag2]]\",  ..." }
    rows[#rows + 1] = { "ADD SLAVE TAG $tag_name $backend_idx", "example: \"add slave tag tag_name backend_idx[,backend_idx]\",  ..." }
    rows[#rows + 1] = { "REMOVE SLAVE TAG $tag_name $backend_idx", "example: \"add slave tag tag_name backend_idx[,backend_idx]\",  ..." }
    rows[#rows + 1] = { "ALTER SLAVE WEIGHT $backendIndx $weight", "example: \"alter slave weight backendIndx weight\",  ..." }
    rows[#rows + 1] = { "ADD ADMIN USER HOSTS $ips", "example: \"add admin user host ip1[,ip2,...]\",  ..." }
    rows[#rows + 1] = { "REMOVE ADMIN USER HOSTS $ips", "example: \"remove admin user host ip1[,ip2...]\",  ..." }
    rows[#rows + 1] = { "ALTER ADMIN USER $pwd", "example: \"alter admin user user:raw_password\",  ..." }
    rows[#rows + 1] = { "SAVE CONFIG", "save the backends to config file" }
    rows[#rows + 1] = { "SELECT VERSION", "display the version of dbproxy" }

    rows[#rows + 1] = { "SHOW proxy STATUS", "list the status or variables" }
        rows[#rows + 1] = { "SHOW processlist", "list the connections and their status" }
        rows[#rows + 1] = { "SHOW events waits STATUS", "list the statistics of the wait event" }
        rows[#rows + 1] = { "SHOW query_response_time", "list the statistics of the query response time" }
        rows[#rows + 1] = { "SHOW blacklists", "list the content of the blacklist" }
        rows[#rows + 1] = { "SHOW TABLES [ LIKE '$table_name[%]']", "list the content of sharding table." }
        rows[#rows + 1] = { "ADD TABLES '$table_name'", "add sharding table_name: db.tbl.col.shard_num." }
        rows[#rows + 1] = { "REMOVE TABLES [ LIKE '$table_name[%]']", "list the content of sharding table." }
        rows[#rows + 1] = { "SHOW VARIABLES [ LIKE '$var_name[%]' ]", "list the variables value" }
        rows[#rows + 1] = { "CLEAR blacklists", "clear the content of the blacklist" } 
        rows[#rows + 1] = { "LOAD blacklists", "load the content of the blacklist from blacklist_file" }
        rows[#rows + 1] = { "SAVE blacklists", "save the content of the blacklist to blacklist_file" }
        rows[#rows + 1] = { "ADD blacklist 'sql_raw' [0|1]", "add new blacklist" }
        rows[#rows + 1] = { "REMOVE blacklist 'hash_code'", "remove blacklist" }
        rows[#rows + 1] = { "SET blacklist 'hash_code' 0|1", "update blacklist status" }
        rows[#rows + 1] = { "SHOW lastest_queries", "list the lastest queries" }
        rows[#rows + 1] = { "SET sql-log = ON|OFF|REALTIME", "online set sql-log option" }
        rows[#rows + 1] = { "SET sql-log-max-size = $int", "online set sql-log-max-size in bytes" }
        rows[#rows + 1] = { "SET sql-log-file-num  = $int", "online set sql-log-file-num" }
        rows[#rows + 1] = { "SET log-level = DEBUG|INFO|MESSAGE|WARNING|CRITICAL|ERROR", "online set log-level option" }
        rows[#rows + 1] = { "SET sql-log-mode=ALL|CLIENT|BACKEND", "online set sql log mode" }
        rows[#rows + 1] = { "SET log-trace-modules = $int", "online set debug trace modules, combinated value of : none: 0x00 connection_pool:0x01 event:0x02 sql: 0x04 con_status:0x08 shard: 0x10 all:0x1F"}
        rows[#rows + 1] = { "SET lastest-query-num = $int", "online set reserved queries number" }
        rows[#rows + 1] = { "SET query-filter-time-threshold = $int", "online set filter query time threshold" }
        rows[#rows + 1] = { "SET query-filter-frequent-threshold = $float", "online set filter query's frequency threshold(access times per-second)" }
        rows[#rows + 1] = { "SET access-num-per-time-window = $int", "online set the query' threashold of accessing times." }
        rows[#rows + 1] = { "SET auto-filter-flag = on|off", "online set auto added filter's flag" } 
        rows[#rows + 1] = { "SET manual-filter-flag = on|off", "online set manual added filter's default flag" } 
        rows[#rows + 1] = { "SET blacklist-file = $file_path", "online set blacklist file" } 
        rows[#rows + 1] = { "SET backend-max-thread-running = $int", "online set backend's max thread running number" }
        rows[#rows + 1] = { "SET thread-running-sleep-delay = $int", "online set backend's max thread running sleep timeout" }
        rows[#rows + 1] = { "SET shutdown-timeout = $int", "online set the waiting seconds of idle connections during shutdown process" }
        rows[#rows + 1] = { "SHUTDOWN [NORMAL] | IMMEDIATE", "online shutdown the dbproxy Server, NORMAL: wait for the current transaction complete before shutdown-timeout expired, IMMEDIATE: shutdown immediately" }
        rows[#rows + 1] = { "KILL [CONNECTION] $id", "online kill the client connection, the $id can be found in command show processlist." }
        rows[#rows + 1] = { "SHOW percentile [$int m|h]", "display the response time, m:minute h:hour" }
        rows[#rows + 1] = { "SET percentile-switch = on|off", "online set percentile" }
        rows[#rows + 1] = { "SET percentile-value = $int (0,100]", "set the percentile" }
    else
        set_error("use 'SELECT * FROM help' to see the supported commands", 1)
        return proxy.PROXY_SEND_RESULT
    end

    proxy.response = {
        type = proxy.MYSQLD_PACKET_OK,
        resultset = {
            fields = fields,
            rows = rows
        }
    }
    return proxy.PROXY_SEND_RESULT
end
