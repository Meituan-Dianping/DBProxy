--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2012, Oracle and/or its affiliates. All rights reserved.

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

-- vim:sw=4:noexpandtab 

---
-- a lua baed test-runner for the mysql-proxy
--
-- to stay portable it is written in lua
--

-- we require LFS (LuaFileSystem)
require("lfs")
require("glib2")
require("posix")

---
-- get the directory-name of a path
--
-- @param filename path to create the directory name from
function dirname(filename)
    local dirname = filename

    attr = assert(lfs.attributes(dirname))

    if attr.mode == "directory" then
        return dirname
    end

    dirname = filename:gsub("/[^/]+$", "")
    
    attr = assert(lfs.attributes(dirname))

    assert(attr.mode == "directory", "dirname("..filename..") failed: is ".. attr.mode)

    return dirname
end

---
-- get the file-name of a path
--
-- @param filename path to create the directory name from
function basename(filename)
    name = filename:gsub(".*/", "")
    
    return name
end

-- 
-- a set of user variables which can be overwritten from the environment
--

local testdir = dirname(arg[0])

dofile(testdir .. "/test.configure")

local pos = string.find(PROXY_BACKEND_ADDRESS, ":")
MYSQL_HOST = string.sub(PROXY_BACKEND_ADDRESS, 1, pos-1)
MYSQL_PORT = string.sub(PROXY_BACKEND_ADDRESS, pos+1, #PROXY_BACKEND_ADDRESS)

pos = string.find(ADMIN_PWDS, ":")
ADMIN_USER = string.sub(ADMIN_PWDS, 1, pos-1)
ADMIN_PASSWORD = string.sub(ADMIN_PWDS, pos+1, #ADMIN_PWDS)

local port_base  = PROXY_PORT_BASE or 32768
PROXY_PORT         = tostring(port_base + 0)
PROXY_MASTER_PORT  = tostring(port_base + 10)
PROXY_SLAVE_PORT   = tostring(port_base + 20)
PROXY_CHAIN_PORT   = tostring(port_base + 30)
ADMIN_PORT         = tostring(port_base + 15)
ADMIN_MASTER_PORT  = tostring(port_base + 25)
ADMIN_SLAVE_PORT   = tostring(port_base + 35)
ADMIN_CHAIN_PORT   = tostring(port_base + 45)

local srcdir         = testdir .. "/"
local top_builddir   = testdir .. "/../"
local builddir       = testdir .. "/" -- same as srcdir by default

local PROXY_BINPATH  = PROXY_INSTALL_PATH .. "/bin/mysql-proxy"
PROXY_LIBPATH  = PROXY_INSTALL_PATH .. "/lib/mysql-proxy/plugins/"

--
-- end of user-vars
--
PROXY_TEST_BASEDIR      = lfs.currentdir()

default_proxy_options = {
    ["proxy-backend-addresses"] = PROXY_BACKEND_ADDRESS or "localhost:5002",
    ["proxy-read-only-backend-addresses"] = PROXY_READONLY_BACKEND_ADDRESSES or "",
    ["proxy-address"]           = "0.0.0.0" .. ":" .. PROXY_PORT,
    ["pwds"]                    = PROXY_PWDS or "root:",
    ["admin-address"]           = "0.0.0.0" .. ":" .. ADMIN_PORT,
    ["admin-username"]          = ADMIN_USER or "root",
    ["admin-password"]          = ADMIN_PASSWORD or "",
    ["plugin-dir"]              = PROXY_LIBPATH,
    ["basedir"]                 = PROXY_INSTALL_PATH,
    ["log-level"]               = "message",
    ["event-threads"]           = EVENT_THREADS or "8",
    ["select-where-limit"]      = SELECT_WHERE_LIMIT or "false",
    ["keepalive"]               = KEEP_ALIVE or "true",
    ["daemon"]                  = DAEMON or "true",
    ["sql-log"]                 = SQL_LOG or "ON"
    }


tests_to_skip = {}
local tests_to_skip_filename = 'tests_to_skip.lua'
local proxy_list = {}
default_proxy_name = 'default'
local default_proxy_conf_name = PROXY_DEFAULT_FILE

local exitcode=0

---
-- print_verbose()
--
-- prints a message if either the DEBUG or VERBOSE variables
-- are set.
--
-- @param msg the message being printed
-- @param min_level the minimum verbosity level for printing the message (default 1)
function print_verbose(msg, min_level)
    min_level = min_level or 1
    if (VERBOSE >= min_level) then
        print (msg)
    end
end
---
-- check if the file exists and is readable 
function file_exists(f)
    return lfs.attributes(f)
end

---
-- create the default option file
--
function make_default_options_file(fname)
    if file_exists(fname) then
        return
    end
    local fd = assert(io.open(fname, "w"))
    fd:write('start_proxy(default_proxy_name) \n')
    fd:close();
end

--- 
-- copy a file
--
-- @param dst filename of the destination
-- @param src filename of the source
function file_copy(dst, src, bappend)
    -- print_verbose("copying ".. src .. " to " .. dst)
    local src_fd = assert(io.open(src, "rb"))
    local content = src_fd:read("*a")
    src_fd:close();

    local dst_fd;
    if not bappend then
        dst_fd = assert(io.open(dst, "wb+"))
    else
        dst_fd = assert(io.open(dst, "a"))
    end
    dst_fd:write(content);
    dst_fd:close();
end

---
-- create a empty file
--
-- if the file exists, it will be truncated to 0
--
-- @param dst filename to create and truncate
function file_empty(dst)
    -- print_verbose("emptying " .. dst)
    local dst_fd = assert(io.open(dst, "wb+"))
    dst_fd:close();
end

---
-- create the default conf file
--
function make_conf_file(fname, option_tbl, conf_file, global_conf_file, b_default_option)
    if file_exists(fname) then
        os.remove(fname)
    end

    local fd;

    if conf_file and file_exists(conf_file) then
        print_verbose("copy conf_file: " .. conf_file .. " to " .. fname)
        file_copy(fname, conf_file, false)
    else
        file_empty(fname);
    end

    if global_conf_file and file_exists(global_conf_file) then
        print_verbose("copy global_conf_file: " .. global_conf_file .. " to " .. fname)
        file_copy(fname, global_conf_file, true)
        fd = assert(io.open(fname, "a"))
    else
        fd = assert(io.open(fname, "a"))
        fd:write("\n[mysql-proxy]\n")
    end

    for k, v in pairs(option_tbl) do
        local s = ""
        if type(v) == "table" then
            values = v
        else
            values = { v }
        end

        for i, tv in ipairs(values) do
            if i > 1 then
                s = s .. ", " .. tv
            else
                s = s .. tv
            end
        end
        fd:write(k .. " = " .. s .."\n")
    end

    if not b_default_option then
        for k, v in pairs(default_proxy_options) do
            if option_tbl[k] == nil then
                local s = ""
                if type(v) == "table" then
                    values = v
                else
                    values = { v }
                end

                for i, tv in ipairs(values) do
                    if i > 1 then
                        s = s .. ", " .. tv
                    else
                        s = s .. tv
                    end
                end
                fd:write(k .. " = " .. s .."\n")
            end
        end
    end

    fd:close();
end

---
-- turn a option-table into a string 
--
-- the values are encoded and quoted for the shell
--
-- @param tbl a option table
-- @param sep the seperator, defaults to a space
function options_tostring(tbl, sep)
    -- default value for sep 
    sep = sep or " "
    
    assert(type(tbl) == "table")
    assert(type(sep) == "string")

    local s = ""
    for k, v in pairs(tbl) do
        local values
        -- if the value is a table, repeat the option
        if type(v) == "table" then
            values = v
        else
            values = { v }
        end

        for tk, tv in pairs(values) do
            local enc_value = tv:gsub("\\", "\\\\"):gsub("\"", "\\\"")
            s = s .. "--" .. k .. "=\'" .. enc_value .. "\' "
        end
    end
    -- print_verbose(" option: " .. s)

    return s
end

--- turns an option table into a string of environment variables
--
function env_options_tostring(tbl)
    assert(type(tbl) == "table")

    local s = ""
    for k, v in pairs(tbl) do
        local enc_value = v:gsub("\\", "\\\\"):gsub("\"", "\\\"")
        s = s .. k .. "=\"" .. enc_value .. "\" "
    end

    return s
end


function os_execute(cmdline)
    print_verbose("$ " .. cmdline)
    return os.execute(cmdline)
end

---
-- get the PID from the pid file
--
function get_pid(pid_file_name)
    -- the file may exist, but the PID may not be written yet
    local pid
    local rounds = 0

    repeat 
        local fh = assert(io.open(pid_file_name, 'r'))
        pid = fh:read("*n")
        fh:close()
        if not pid then
            glib2.usleep(200 * 1000) -- wait a bit until we get some content
            rounds = rounds + 1

            if rounds > 10 then
                error(("reading PID from existing pid-file '%s' failed after waiting 2 sec"):format(
                    pid_file_name))
            end
        end
    until pid

    -- the PID we get here should be a number
    assert(type(pid) == "number")

    return pid
end

function wait_proc_up(pid_file) 
    local rounds = 0

    while not file_exists(pid_file) do
        glib2.usleep(200 * 1000) -- wait until the pid-file is created

        rounds = rounds + 1
        print_verbose(("(wait_proc_up) pid-wait: %d rounds, (%s)"):format(rounds, pid_file))

        if rounds > 1000 then error(("proxy failed to start: no pid-file %s"):format(pid_file)) end
    end

    local pid = get_pid(pid_file)

    rounds = 0
    -- check that the process referenced in the PID-file is still up
    while 0 ~= os.execute("kill -0 ".. pid .."  2> /dev/null") do
        glib2.usleep(200 * 1000) -- wait until the pid-file is created
        rounds = rounds + 1
        print_verbose(("(wait_proc_up) kill-wait: %d rounds, pid=%d (%s)"):format(rounds, pid, pid_file))

        if rounds > 5 then error(("proxy seems to have crashed: pid=%d (%s)"):format(pid, pid_file)) end
    end
end

function proc_is_up(pid)
    return os.execute("kill -0 ".. pid .."  2> /dev/null")
end

function proc_stop(pid)
    return os.execute("kill -TERM ".. pid)
end

function wait_proc_down(pid_file) 
    local rounds = 0
    local pid = get_pid(pid_file)

    -- wait until the proc in the pid file is dead
    -- the shutdown takes at about 500ms
    while 0 == proc_is_up(pid) do
        glib2.usleep(200 * 1000) -- wait until process is gone
        rounds = rounds + 1
        print_verbose(("(wait_proc_down) kill-wait: %d rounds, pid=%d (%s)"):format(rounds, pid, pid_file))
    end
end

function stop_proxy()
    -- shut dowm the proxy
    --
    -- win32 has tasklist and taskkill on the shell
     
    
    -- shuts down every proxy in the proxy list
    --
    for proxy_name, proxy_options in pairs(proxy_list) do
        local pid
        pid_file = proxy_options["pid-file"]
        print_verbose ('stopping proxy ' .. proxy_name)
        
        pid = get_pid(pid_file)

        if proc_is_up(pid) then
            proc_stop(pid)
        else
            print("-- process "..proxy_name.." is already down")
            exitcode = -1
        end
    end

    -- wait until they are all gone
    for proxy_name, proxy_options in pairs(proxy_list) do
        pid_file = proxy_options["pid-file"]

        wait_proc_down(pid_file)

        os.remove(pid_file)
    end

    --
    -- empties the proxy list
    --
    proxy_list = { }
end

function only_item ( tbl, item)
    local exists = false
    for i,v in pairs(tbl) do
        if i == item then
            exists = true
        else
            return false
        end
    end
    return exists
end

---
-- before_test()
--
-- Executes a script with a base name like test_name and extension ".options"
--
-- If there is no such file, the default options are used
--
function before_test(basedir, test_name)
    local options_filename = basedir .. "/t/" .. test_name .. ".options"
    local has_option_file = file_exists(options_filename)

    global_basedir = basedir
    print_verbose ('current_dir ' ..  
                    basedir)
    -- 
    -- executes the content of the options file
    --
    if has_option_file then
        print_verbose('# using options file ' .. options_filename)
        stop_proxy()
    else
        -- 
        -- if no option file is found, the default options file is executed
        --
        options_filename = basedir .. "/t/default.options"
        print_verbose('#using default options file' .. options_filename)
        if only_item(proxy_list,'default') then
            print_verbose('reusing existing proxy')
            return
        end
        make_default_options_file(options_filename)
    end
    assert(loadfile(options_filename))()
end

function after_test()
    if only_item(proxy_list, 'default') then
        return
    end
    stop_proxy()
end

function alternative_execute (cmd)
    print_verbose(cmd)

    local fh = io.popen(cmd)
    assert(fh, 'error executing '.. cmd)
    local result = ''
    local line = fh:read()
    while line do
        result = result .. line
        line = fh:read()
    end
    fh:close()
    return result
end

function conditional_execute (cmd)
    if USE_POPEN then
        return alternative_execute(cmd)
    else
        return os_execute(cmd)
    end
end

--- 
-- run a test
--
-- @param testname name of the test
-- @return exit-code of mysql-test
function run_test(filename, basedir)
    local testname = assert(filename:match("t/(.+)\.test"))
    if tests_to_skip[testname] then
        print('skip ' .. testname ..' '.. (tests_to_skip[testname] or 'no reason given') )
        return 0, 1
    end
    before_test(basedir, testname)

    if VERBOSE > 1 then     
        os.execute('echo -n "' .. testname  .. ' " ; ' )
    end
    local result = 0
    local ret = 'ok'
    if RUN_MODE == 'setup' then
        ret = conditional_execute(
                  env_options_tostring({
                        ['MYSQL_USER']  = MYSQL_USER,
                        ['MYSQL_PASSWORD']  = MYSQL_PASSWORD,
                        ['PROXY_HOST']  = PROXY_HOST,
                        ['PROXY_PORT']  = PROXY_PORT,
                        ['PROXY_CHAIN_PORT']  = PROXY_CHAIN_PORT,
                        ['MASTER_PORT'] = PROXY_MASTER_PORT,
                        ['SLAVE_PORT'] = PROXY_SLAVE_PORT,
                }) .. ' ' ..
                MYSQL_TEST_BIN .. " -r " ..
                options_tostring({
                        user     = MYSQL_USER,
                        password = MYSQL_PASSWORD,
                        database = MYSQL_DB,
                        host     = PROXY_HOST,
                        port     = PROXY_PORT,
                        verbose  = (VERBOSE > 0) and "TRUE" or "FALSE", -- pass down the verbose setting
                        ["test-file"] = basedir .. "/t/" .. testname .. ".test",
                        ["result-file"] = basedir .. "/r/" .. testname .. ".result",
                        ["logdir"] = basedir .. "/log/", -- the .result dir might not be writable
                        ["protocol"] = "tcp", -- the .result dir might not be writable
                })
        )
    elseif RUN_MODE == 'admin' then
        ret = conditional_execute(
                  env_options_tostring({
                        ['MYSQL_USER']  = ADMIN_USER,
                        ['MYSQL_PASSWORD']  = ADMIN_PASSWORD,
                        ['PROXY_HOST']  = PROXY_HOST,
                        ['PROXY_PORT']  = ADMIN_PORT,
                        ['PROXY_CHAIN_PORT']  = PROXY_CHAIN_PORT,
                        ['MASTER_PORT'] = PROXY_MASTER_PORT,
                        ['SLAVE_PORT'] = PROXY_SLAVE_PORT,
                }) .. ' ' ..
                MYSQL_TEST_BIN .. " -r " ..
                options_tostring({
                        user     = ADMIN_USER,
                        password = ADMIN_PASSWORD,
                        host     = PROXY_HOST,
                        port     = ADMIN_PORT,
                        verbose  = (VERBOSE > 0) and "TRUE" or "FALSE", -- pass down the verbose setting
                        ["test-file"] = basedir .. "/t/" .. testname .. ".test",
                        ["result-file"] = basedir .. "/r/" .. testname .. ".result",
                        ["logdir"] = basedir .. "/log/", -- the .result dir might not be writable
                        ["protocol"] = "tcp", -- the .result dir might not be writable
                })
        )
    else
        if RUN_MODE == 'check' then
                ret = conditional_execute(
                    MYSQL_TEST_BIN .. " -r " ..
                    options_tostring({
                        user     = MYSQL_USER,
                        password = MYSQL_PASSWORD,
                        database = MYSQL_DB,
                        host     = MYSQL_HOST,
                        port     = MYSQL_PORT,
                        verbose  = (VERBOSE > 0) and "TRUE" or "FALSE", -- pass down the verbose setting
                        ["test-file"] = basedir .. "/t/" .. testname .. ".test",
                        ["result-file"] = basedir .. "/r/" .. testname .. ".result",
                        ["logdir"] = basedir .. "/log/", -- the .result dir might not be writable
                        ["protocol"] = "tcp", -- the .result dir might not be writable
                   })
               )
        end

        ret = conditional_execute(
        env_options_tostring({
            ['MYSQL_USER']  = MYSQL_USER,
            ['MYSQL_PASSWORD']  = MYSQL_PASSWORD,
            ['PROXY_HOST']  = PROXY_HOST,
            ['PROXY_PORT']  = PROXY_PORT,
            ['PROXY_CHAIN_PORT']  = PROXY_CHAIN_PORT,
            ['MASTER_PORT'] = PROXY_MASTER_PORT,
            ['SLAVE_PORT'] = PROXY_SLAVE_PORT,
        }) .. ' ' ..
        MYSQL_TEST_BIN .. " " ..
        options_tostring({
            user     = MYSQL_USER,
            password = MYSQL_PASSWORD,
            database = MYSQL_DB,
            host     = PROXY_HOST,
            port     = PROXY_PORT,
            verbose  = (VERBOSE > 0) and "TRUE" or "FALSE", -- pass down the verbose setting 
            ["test-file"] = basedir .. "/t/" .. testname .. ".test",
            ["result-file"] = basedir .. "/r/" .. testname .. ".result",
            ["logdir"] = basedir .. "/log/", -- the .result dir might not be writable
            ["protocol"] = "tcp", -- the .result dir might not be writable
        })
        )
    end

    if USE_POPEN then
        if (ret == 'ok' or ret =='skipped') then
            result = 0
        else
            result = 1
        end
        print(ret .. ' ' .. testname)  
    else
        result = ret
    end
    after_test()    
    return result, 0
end

---
--sql_execute()
--
-- Executes a SQL query in a given Proxy
-- 
-- If no Proxy is indicated, the query is passed directly to the backend server
--
-- @param query A SQL statement to execute, or a table of SQL statements
-- @param proxy_name the name of the proxy that executes the query
function sql_execute(queries, proxy_name)
    local ret = 0
    assert(type(queries) == 'string' or type(queries) == 'table', 'invalid type for query' )
    if type(queries) == 'string' then
        queries = {queries}
    end
    local query = ''
    for i, q in pairs(queries) do
        query = query .. ';' .. q
    end

    if proxy_name then  
        -- 
        -- a Proxy name is passed. 
        -- The query is executed with the given proxy
        local opts = proxy_list[proxy_name]
        assert(opts,'proxy '.. proxy_name .. ' not active')
        assert(opts['proxy-address'],'address for proxy '.. proxy_name .. ' not found')
        local p_host, p_port = opts['proxy-address']:match('(%S+):(%S+)')
        ret = os_execute( MYSQL_CLIENT_BIN .. ' ' ..
            options_tostring({
                user     = MYSQL_USER,
                password = MYSQL_PASSWORD,
                database = MYSQL_DB,
                host     = p_host,
                port     = p_port,
                execute  = query
            })
        )
        assert(ret == 0, 'error using mysql client ')
    else
        --
        -- No proxy name was passed.
        -- The query is executed in the backend server
        --
        ret = os_execute( MYSQL_CLIENT_BIN .. ' ' ..
            options_tostring({
                user     = MYSQL_USER,
                password = MYSQL_PASSWORD,
                database = MYSQL_DB,
                host     = MYSQL_HOST,
                port     = MYSQL_PORT,
                execute  = query
            })
        )
    end
    return ret
end

stop_proxy()

-- the proxy needs the lua-script to exist
-- file_empty(PROXY_TMP_LUASCRIPT)

-- if the pid-file is still pointing to a active process, kill it
--[[
if file_exists(PROXY_PIDFILE) then
    os.execute("kill -TERM `cat ".. PROXY_PIDFILE .." `")
    os.remove(PROXY_PIDFILE)
end
--]]

if COVERAGE_LCOV then
    -- os_execute(COVERAGE_LCOV .. 
    --  " --zerocounters --directory ".. srcdir .. "/../src/" )
end

-- setting the include path
--

-- this is the path containing the global Lua modules
local GLOBAL_LUA_PATH = os.getenv('LUA_LDIR')  or '/usr/share/lua/5.1/?.lua'

-- this is the path containing the Proxy libraries 
local PROXY_LUA_PATH = os.getenv('LUA_PATH')  or '/usr/local/share/?.lua'

-- This is the path with specific libraries for the test suite
local PRIVATE_LUA_PATH = arg[1]  .. '/t/?.lua'  

-- This is the path with additional libraries that the user needs
local LUA_USER_PATH = os.getenv('LUA_USER_PATH')  or '../lib/?.lua'

-- Building the final include path
local INCLUDE_PATH = 
        LUA_USER_PATH    .. ';' ..
        PRIVATE_LUA_PATH  .. ';' ..
        GLOBAL_LUA_PATH   .. ';' .. 
        PROXY_LUA_PATH 

---
-- start_proxy()
--
-- starts an instance of MySQL Proxy
--
-- @param proxy_name internal name of the proxy instance, for retrieval
-- @param proxy_options the options to start the Proxy
function start_proxy(proxy_name, proxy_options, conf_file)
    local b_default_option = false
    local global_conf_file = nil;

    if nil == proxy_options then
        proxy_options = default_proxy_options
        b_default_option = true
    end
    -- start the proxy
    assert(type(proxy_options) == 'table')

    if conf_file then
        conf_file =  (global_basedir or srcdir) .. "/t/" .. conf_file
    end

    if default_proxy_conf_name then
        global_conf_file =  (global_basedir or srcdir) .. "/" .. default_proxy_conf_name
    end

    conf_filename =  PROXY_INSTALL_PATH .. "/conf/" .. proxy_name .. "_test.cnf"

    if  nil == proxy_options["log-path"] then
    proxy_options["log-path"] = PROXY_INSTALL_PATH .. "/log/"
    end

    proxy_options["instance"] = proxy_name .. "_test"

    make_conf_file(conf_filename, proxy_options, conf_file, global_conf_file, b_default_option)

    proxy_options["pid-file"] = proxy_options["log-path"] .. "/" .. proxy_name .. "_test.pid"

    print_verbose("starting " .. proxy_name .. " with " .. PROXY_BINPATH .. " --defaults-file= " .. conf_filename)

    -- remove the old pid-file if it exists
    os.remove(proxy_options["pid-file"])

    if USE_VALGRIND then
            PROXY_TRACE = "LD_PRELOAD=\"" .. PROXY_LIBPATH .. "/libproxy.so " .. PROXY_LIBPATH .. "/libadmin.so\" " .. "G_DEBUG=gc-friendly G_SLICE=always-malloc valgrind --log-file=" .. (VALGRIND_OUTPUT_FILE or "./valgrind.log") .. "_" .. os.time() .. " -v --tool=memcheck --leak-check=full --track-origins=yes"
        end

    assert(os.execute( 'LUA_PATH="' .. INCLUDE_PATH  .. '"  ' ..
        PROXY_TRACE .. " " .. PROXY_BINPATH .. " --defaults-file=" .. conf_filename .. " &"
    ))

    -- wait until the proxy is up
    wait_proc_up(proxy_options["pid-file"])

    glib2.usleep(5000 * 1000) -- wait until the proxy startup

    proxy_list[proxy_name] = proxy_options 
end

---
-- chain_proxy()
--
-- starts two proxy instances, with the first one is pointing to the 
-- default backend server, and the second one (with default ports)
-- is pointing at the first proxy
--
--   client -> proxy -> backend_proxy -> [ mysql-backend ]
--
-- usually we use it to mock a server with a lua script (...-mock.lua) and
-- the script under test in the proxy (...-test.lua)
--
-- in case you want to start several backend_proxies, just provide a array
-- as first param
--
-- @param backend_lua_script
-- @param second_lua_script 
-- @param use_replication uses a master proxy as backend 
function chain_proxy (backend_lua_scripts, second_lua_script, use_replication)
    local backends = { }

    local backend_addresses = { }
    local admin_address

    for i, backend_lua_script in ipairs(backends) do
        backend_addresses[i] = PROXY_HOST .. ":" .. (PROXY_CHAIN_PORT + i - 1)
        admin_address = PROXY_HOST .. ":" .. (PROXY_CHAIN_PORT + i - 1)


        backend_proxy_options = {
            ["proxy-backend-addresses"] = PROXY_BACKEND_ADDRESS or "localhost:5002",
            ["proxy-address"]           = backend_addresses[i],
            ["pwds"]                    = PROXY_PWDS or "root:",
            ["admin-address"]           = "0.0.0.0" .. ":" .. (ADMIN_CHAIN_PORT + i - 1),
            ["admin-username"]          = ADMIN_USER or "root",
            ["admin-password"]          = ADMIN_PASSWORD or "",
            ["plugin-dir"]              = PROXY_LIBPATH,
            ["basedir"]                 = PROXY_TEST_BASEDIR,
            ["log-level"]               = (VERBOSE == 4) and "debug" or "critical",
            ["event-threads"]           = EVENT_THREADS or "8",
            ["select-where-limit"]      = SELECT_WHERE_LIMIT or "false",
            ["keepalive"]               = KEEP_ALIVE or "true",
            ["daemon"]                  = DAEMON or "true",
        }
        start_proxy('backend_proxy' .. i, backend_proxy_options) 
    end

    second_proxy_options = {
            ["proxy-backend-addresses"] = backend_addresses,
            ["proxy-address"]           = PROXY_HOST .. ":" .. PROXY_PORT,
            ["pwds"]                    = PROXY_PWDS or "root:",
            ["admin-address"]           = "0.0.0.0" .. ":" .. (ADMIN_PORT),
            ["admin-username"]          = ADMIN_USER or "root",
            ["admin-password"]          = ADMIN_PASSWORD or "",
            ["plugin-dir"]              = PROXY_LIBPATH,
            ["basedir"]                 = PROXY_TEST_BASEDIR,
            ["log-level"]               = (VERBOSE == 4) and "debug" or "critical",
            ["event-threads"]           = EVENT_THREADS or "8",
            ["select-where-limit"]      = SELECT_WHERE_LIMIT or "false",
            ["keepalive"]               = KEEP_ALIVE or "true",
            ["daemon"]                  = DAEMON or "true",
    }
    start_proxy('second_proxy',second_proxy_options) 
end

local num_tests     = 0
local num_passes    = 0
local num_skipped   = 0
local num_fails     = 0
local all_ok        = true
local failed_test   = {}

--
-- if we have a argument, exectute the named test
-- otherwise execute all tests we can find
if #arg then
    for i, a in ipairs(arg) do
        local stat = assert(lfs.attributes(a))

        if file_exists(a .. '/' .. tests_to_skip_filename) then
            assert(loadfile(a .. '/' .. tests_to_skip_filename))()
        end

        -- if it is a directory, execute all of them
        if stat.mode == "directory" then
            os_execute("mkdir " .. a .. "/log")
            for file in lfs.dir(a .. "/t/") do
                if not TESTS_REGEX or file:match(TESTS_REGEX) then
                    local testname = file:match("(.+\.test)$")
            
                    if testname then
                        print_verbose("# >> " .. testname .. " started")
            
                        num_tests = num_tests + 1
                        local r, skipped  = run_test(a .. "t/" .. testname, a)
                        if (r == 0) then
                            num_passes = num_passes + 1 - skipped
                        else
                            num_fails = num_fails + 1
                            all_ok = false
                            table.insert(failed_test, testname)
                        end
                        num_skipped = num_skipped + skipped

                        print_verbose("# << (exitcode = " .. r .. ")" )
            
                        if r ~= 0 and exitcode == 0 then
                            exitcode = r
                        end
                    end
                    if all_ok == false and (not FORCE_ON_ERROR) then
                        break
                    end
                end
            end
        else 
            -- otherwise just this one test
            --
            -- FIXME: base/ is hard-coded for now
            os_execute("mkdir " .. dirname(a)  .. "/../log")
            exitcode, skipped = run_test(a, dirname(a)  .. "/../")
            num_skipped = num_skipped + skipped
        end
    end
else
    for file in lfs.dir(srcdir .. "/t/") do
        local testname = file:match("(.+\.test)$")

        if testname then
            print_verbose("# >> " .. testname .. " started")

            num_tests = num_tests + 1
            local r, skipped = run_test("t/" .. testname)
            num_skipped = num_skipped + skipped
            
            print_verbose("# << (exitcode = " .. r .. ")" )
            if (r == 0) then
                num_passes = num_passes + 1 - skipped
            else
                all_ok = false
                num_fails = num_fails + 1
                table.insert(failed_test, testname)
            end
            if r ~= 0 and exitcode == 0 then
                exitcode = r
            end
            if all_ok == false and (not FORCE_ON_ERROR) then
                break
            end
        end
    end
end

if all_ok ==false then
    print ("*** ERRORS OCCURRED - The following tests failed")
    for i,v in pairs(failed_test) do
        print(v )
    end
end

--
-- prints test suite statistics
print_verbose (string.format('tests: %d - skipped: %d (%4.1f%%) - passed: %d (%4.1f%%) - failed: %d (%4.1f%%)',
            num_tests,
            num_skipped,
            num_skipped / num_tests * 100,
            num_passes,
            num_passes / (num_tests - num_skipped) * 100,
            num_fails,
            num_fails / (num_tests  - num_skipped) * 100))

--
-- stops any remaining active proxy
--
stop_proxy()

if COVERAGE_LCOV then
    os_execute(COVERAGE_LCOV .. 
        " --quiet " ..
        " --capture --directory ".. srcdir .. "/../src/" ..
        " > " .. srcdir .. "/../tests.coverage.info" )

    os_execute("genhtml " .. 
        "--show-details " ..
        "--output-directory " .. srcdir .. "/../coverage/ " ..
        "--keep-descriptions " ..
        "--frames " ..
        srcdir .. "/../tests.coverage.info")
end


if exitcode == 0 then
    os.exit(0)
else
    print_verbose("mysql-test exit-code: " .. exitcode)
    os.exit(-1)
end

