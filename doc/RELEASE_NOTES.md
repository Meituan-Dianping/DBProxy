
- Installation Notes
- Functionality Added or Changed
    - 从库流量配置  
      指定查询发送到某个从库
	- 参数动态设置(完善show proxy status/variables)  
	  参数动态的设置: 以及支持save config，动态增加、删除分表
	- 响应时间percentile统计  
	  统计最近时间段DBProxy的响应时间
	- kill session  
	  支持DBProxy的admin接口kill session操作
	- backend平滑上下线  
	支持平滑的backend上下线
	- DBProxy非root用户启动  
	使用非root用户启动
	- admin账号的安全限制  
	admin账号密码的动态修改及host限制
	- 增加异步刷日志的功能  
	增加日志线程，异步刷日志，提高响应时间
    - 支持DBProxy平滑重启功能  
        1. normal：等待所有当前事务结束后退出  
        1, KILL -SIGTERM \`pid of mysql-proxy\`; 2, admin 命令: shutdown [normal]，其中等待过程有超时机制  
        2. immediate：不等待当前事务结束直接退出  
        1, KILL -SIGINT \`pid of mysql-proxy\`; 2, admin 命令: shutdown immediate
        3. 配置参数shutdown\_timeout: 在normal方式下，等待shutdown\_timeout时间后退出，单位是s, 默认值是600.
    - 支持SQL过滤的黑名单功能
      - 添加到黑名单中需要满足两个条件：SQL 执行的时间和频率
        - SQL执行的时间  
            由参数 query-filter-time-threshold来指定，如果SQL执行时间超过此值，则满足条件；
        - SQL执行频率  
            由参数 query-filter-frequent-threshold来指定，如果SQL执行频率超过此值，则满足条件；  
            频率就是是在时间窗口内执行的次数。时间窗口则是由频率阈值和最小执行次数来计算出来的，当时间窗口小于60s时，扩展到60s。  
            参数access-num-per-time-window用来指定在时间窗口内的最小执行次数，添加此参数是考虑到执行时间长的SQL在计算频率时同时参考其执行的次数，只有执行一定次数时才去计算其频率。当执行时间与执行频率都满足时条件时，会自动将查询作为过滤项放到黑名单中，加入到黑名单中是否生效由参数 auto-filter-flag 来控制，OFF:不生效，ON:立即生效。
        - 黑名单的管理
            - 提供了查看，修改，添加，删除黑名单的功能。
            - 黑名单管理提供了将黑名单保存到文件以及从文件中Load到内存中的功能。
            - 在手动添加黑名单时，只需要将用户的SQL语句输入，在内部自动转化成过滤条件，手动添加时是否生效由参数manual-filter-flag来控制，OFF:不生效，ON:立即生效。
            - 手动添加与自动添加两种情况下的过滤条件是否生效是分别由不同参数控制，这个要区分清楚。另外，也可以使用 admin 的命令来设置是否开启/关闭某个过滤条件。
    - 支持对于MySQL后台的thread running限制功能  
    该功能通过在DBProxy内限制每个后台MySQL的并发查询来控制对应MySQL的thread running数。  
    当发向某个MySQL后台的的并发查询超过某个阈值时，会进行超时等待，直到有可用的连接，其中阈值与超时等待的时间都已经参数化，可以动态配置。
        - 新增参数  
            - backend-max-thread-running用于指定每个MySQL后台的最大thread running数
            - thread-running-sleep-delay用于指定在thread running数超过backend-max-thread-running时，客户端连接等待的时间。
    - set backend offline不再显示节点状态
    - 支持set transaction isolation level
    - 支持use db
    - 支持set option语句
    - 支持set session级系统变量
    - 支持建立连接时指定连接属性。
    - 改进连接池的连接管理，增加超时释放机制。当连接池中的空闲连接闲置超过一定时间后，自动释放连接。由参数db-connection-idle-timeout控制。
    - 增加客户端连接的keepalive机制，避免网络异常后释放已断开的连接。
    - 完善管理日志，增加了管理命令日志、错误语句日志以及详细的错误日志。
    - 完善SQL日志信息，包含了详细的连接信息，并包含了DBProxy内部执行的隐式SQL语句。隐式SQL语句主要是连接重用时切换database、字符集的语句。
    - 增加SQL日志rotate机制，可设置日志文件最大大小和日志文件最大个数，自动清理早期的SQL日志。分别由参数sql-log-file-size和sql-log-file-num控制。
    - 增加后台MySQL版本号设置，主要影响MySQL连接协议中的server版本，客户端驱动可能依赖于server版本处理机制有所不同。由参数mysql-version控制。
    - 性能改进，将SQL词法分析从串行方式改进为并发方式；其次，在每次执行SQL前如果database相同时，不再需要执行COM_INIT_DB命令。根据测试结果，在特定环境下sysbench的QPS从7万提升至22万。
	- 增加监控统计信息，包括连接状态、QPS、响应时间、网络等统计
	- sql log动态配置
	- 改进autocommit为false时频繁连接主库的问题
- Bugs Fixed
	- DBProxy建立连向MySQL连接时，新建的socket添加keepalive和非阻塞的属性
	- rpm安装时，创建conf目录并创建默认的配置文件的功能
	- rpm安装时，需要手动修改mysql-proxyd文件中的proxy-dir, 现在直接在rpm安装后就修改好。
	- 解决了绑定后端连接断开时，客户端连接未及时断开的问题。
    - 屏蔽了KILL语句，避免在后端MySQL可能误KILL的问题。
    - 修改了事务内语句执行错误时，DBProxy未保留后台连接导致rollback发送到其它结点的问题
    - 修复分表查询结果合并时列字符集错误的问题，该问题可能会导致结果乱码。
    - 解决在分表情况下，返回值有 NULL 的情况下，查询超时的问题  
    此问题是DBProxy在多个分表merge结果的过程中未处理 NULL 值，导致结果集返回不对，而JDBC接口会认为此种情况下是未收到结果，会处于一直等待状态，触发超时。
    - 解决在分表情况下， IN 子句中分表列只支持 int32，不支持int64 的问题
    - 解决连接断开的内存泄露问题  
    在连接的结构体的释放接口中，lock 的成员变量未释放，导致在连接断开，回收连接对象时会泄漏24个字节。
    - 取消admin操作中不必要的日志  
    - 去掉了在连接 admin 时报"[admin] we only handle text-based queries (COM_QUERY)"的信息，此信息属于无用的信息。  
    - 去掉了在set backend offline/online时的返回值信息，此信息是无用信息
    - 解决用户权限不足、DBProxy用户名密码配置错误等导致使用错误用户的问题。
    - 解决SQL_CALC_FOUND_ROWS之后SQL语句发往主库的问题。
    - 解决SQL语句中有注释时语句分析不正确的问题。
    - 解决客户端发送空串导致DBProxy挂掉的问题。
- Migration
- TODO
