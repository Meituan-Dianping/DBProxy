## DBProxy FAQ


> 该文档中的FAQ主要整理自DBProxy用户QQ交流群(367199679)大家的交流内容，将共性的问题进行整理、积累，方便大家回顾和学习。持续更新中...


### Q1：公司内部使用的DBProxy相对于360开源的Atlas做了哪些改进？

详见 [release_notes](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/RELEASE_NOTES.md)

### Q2：CentOS/Ubuntu/Debian默认源中glib2的版本是2.28.8-4.el6，会导致DBProxy make报错，如何获取正确glib2版本？

[glib-2.4.2.0.tar.xz download](http://pkgs.fedoraproject.org/repo/pkgs/mingw-glib2/glib-2.42.0.tar.xz/71af99768063ac24033ac738e2832740/)

>#### 编译安装的步骤

>##### (1) 安装依赖项

>###### Ubuntu & Debian

> `apt-get install gettext libffi-dev automake python`

>###### CentOS
> `yum install zlib-devel zlib libffi libffi-devel gettext automake libtool autoconf`

>##### (2) 运行 autoreconf命令

> `autoreconf -ivf`

>##### (3) 运行如下命令进行安装

> `tar xf glib-2.42.0.tar.xz`

> `cd glib-2.42.0`

> `./configure`

> `make`

> `[ Become root if necessary ]`

> `rm -rf /install-prefix/include/glib.h /install-prefix/include/gmodule.h`

> `make install              # install GLIB`

### Q3: DBProxy的后端连接是怎么复用的？代码中没有看到change user这个过程。

DBProxy中，将可复用的连接放回连接池，在连接池中，是按照用户名来管理连接的。如果user1 新建立的连接，DBPRoxy会去分给它的backend的连接池中，找到user1的连接，如果有，则分配，没有则新建立连接。

示意图可以参考  [开发手册](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/PROGRAMMING_GUIDE.md)最后的那张图 

### Q4: DBProxy是如何控制最大连接数的？是否针对不同用户设置最大连接数?

DBProxy中连接池中连接的数量目前还没有限制；目前也没有针对某一用户来统计连接数量，针对某一用户连接数来限制；DBProxy只统计了连接DBProxy的总连接，不区分用户，根据统计的总连接，可以动态设置最大允许连接DBproxy的连接数量。

### Q5: DBProxy 可以在ubuntu server上运行吗？

目前DBProxy 只在CentOS6.5上进行过适配。

需要的环境和依赖库参考[快速入门手册](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/QUICK_START.md)

### Q6: DBProxy的文档在哪里?

详见[README_ZH](https://github.com/Meituan-Dianping/DBProxy/blob/master/README_ZH.md)

### Q7: DBProxy是否可以立即在生产环境使用？

目前开源的DBProxy与美团点评内部大规模生产环境使用的版本一致。

### Q8: DBProxy与MyCat有何异同？

我们没有使用过MyCat产品的经验，所以不好比较，根据网上资料，可能主要功能类似，各有个特点吧。

### Q9: DBProxy对mysql的版本有限制么？

目前测试过mysql5.5 和 mysql5.6。

**需要注意：**DBproxy 配置文件中需要配置下后端mysql的版本，默认5.5 。例如，使用mysql是5.6时候，配置文件中配置参数 mysql-version=5.6。

### Q10: 美团已经把DBProxy 的所有代码都开源了吗？

DBProxy 完全开源了，美团点评 目前内部使用的版本和github上的版本一致。我们后续的所有维护、开发都会直接在github上进行。

### Q11: DBProxy自己管理与MySQL的连接池么？以及如何管理？
是自己管理的。

DBProxy的每个处理线程（一个处理线程可能同时处理多个客户端连接）对于每个后台MySQL都有一个连接池；处理线程首先从连接池中获取后台db连接，如果连接池不存在连接时，则新建后台db连接；使用完成后（语句执行完成后，当前连接不在事务中，而且当前连接没有last_insert_id、found_rows等上下文信息），会将连接放入连接池；连接池中的连接空闲超过一定时间（可由db-connection-idle-timeout配置，默认为1小时）后，DBProxy会自动关闭该链接。

### Q12: DBProxy的连接池如何处理session级变量？
DBProxy会在每个客户端连接记录当前连接设置的session变量状态，同时在每个后台db连接上也会记录该连接设置的session变量状态；当后台db连接被重用时，根据客户端连接和后台db连接的session变量状态，重新设置后台db连接的session变量状态。

### Q13: DBProxy的连接池如何处理事务？
首先客户端连接的整个事务由同一个后台db连接来处理，其次如果客户端连接关闭时仍然在事务中，不会将连接放入连接池而是关闭对应的后台db连接，由MySQL回滚该事务。

### Q14: DBProxy和主mysql放一台物理机对性能影响大吗?

DBProxy消耗资源不大，性能影响不大。

### Q15: DBProxy 前面加个lvs这种方式，程序支持没有问题吧？

没有问题，美团点评也使用lvs。

### Q16: DBProxy 支持分库分表吗？还是只支持分表？

目前开源的版本只支持分表，分库分表版本在内测，稳定后会立即开源。

### Q17: DBProxy 主要是什么语言编写的？

主要是C，有一小部分Lua。

### Q18: DBProxy的负载均衡算法是什么？

现阶段：   
 
- 权重一样，简单的从库轮询（RR）；    

- 权重不同，按权重比例轮询（例如：只有两个可用的slave, slave 1:weight 4  , slave 2: weight 1, 每5次4次发送查询语句到slave1,1次 发送查询语句到slave2）；   
 
- 配置了tag，会根据配置的tag发送到指定的从库（tag详细参考 [从库流量配置](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.3.7.2)）；

- 支持threadrunning功能，进行过载保护（DBProxy会周期获取MySQL的实际threadrunning，根据DBProxy 上配置的threadrunning来选择可用的从库）。

### Q19: DBProxy如果处理客户端已关闭的连接？

正常情况下，客户端在连接断开时会发送FIN报文，DBProxy收到FIN报文后，会自动关闭客户端连接；对于异常情况，首先DBProxy加入了TCP协议的keepalive机制检测错误连接，其次DBProxy会自动关闭空闲超过一定时间（可通过wait-timeout配置，默认为8小时）的连接。

### Q20: DBProxy的日志有几种，如何命名？

DBProxy的日志有两种，第一种是记录DBProxy运行状态的日志，另一种是记录SQL执行情况的日志；记录运行状态的日志类似于mysql 的 error.log, 其命名方式是由配置文件的instance参数指定，类似$instance.log；而记录SQL执行情况的日志类似于MySQL的general log，其命名方式是第一种日志的名称加前缀"sql_"， 类似sql_$instance.log。可以根据具体的情况查找对应的log。
### Q21: DBProxy 日志中常见错误汇总

- I have no server backend, closing connection

>`管理日志`中出现该错误日志时，一般情况下，说明当前的所有从库和主库均不可用（DOWN状态、PENDING状态、OFFLINE状态、OFFLINING/REMOVING状态）。
检查主库和从库状态，是由DBProxy中周期线程check_state完成，周期检查主从数据库的状态。

>#### 造成DOWN状态一般原因：

>后台数据库crash、网络不通、网络延迟、人为将数据库摘除或是设置成DOWN。

>#### 造成PENDING状态一般原因：

>后台数据库压力过大超过设置的threadrunning阈值，进行流量保护。

>#### 造成OFFLINE状态一般原因：(排查问题时极少遇到)

>人为将数据库状态设置成OFFLINE状态。

>#### 造成OFFLINING/REMOVING状态一般原因：（排查问题时极少遇到）

>在通过admin端口设置数据库状态为OFFLINE或摘除数据库时，如果当前有事务中的连接，且没有超过最大等待时间，则会变成中间状态OFFLINING/REMOVING状态，该状态时，同样不会接受新建立的连接的请求。

>#### 排查方法：

>backend的状态被修改时，会打印日志，可以根据日志查看是backend被设置了什么状态导致了该错误：

>`(message)set backend (backend_name) state to UP|DOWN|PENDING`

>如果被设置了DOWN状态，可以继续查找什么原因导致：

>`(warning)set backend(backend_name) state to DOWN for: %mysql_errno(%mysql_errmsg)##由于连接不上MySQL数据库导致的（具体错误见mysql_errmsg）。注意：check_state线程中连接MySQL时，或是获取MySQL threadrunning值时均可能引起该错误`
 
>`(warning)set backend(backend_name) state to DOWN for retry %d times to get thread_running ##由于开启threadrunning功能后，%d次尝试 获得后台MySQL数据库当前threadrunning值均失败导致的`

>#### 特别注意：

>常见的mysql_msg:` MySQL server has gone away `, 导致该错误码的原因之一可能主要是由于DBProxy在连接MySQL时，设置了超时（mysql_options()），该超时参数可以在配置文件中设置（check-state-conn-timeout=1），默认1s。由于网络等原因，引起超时时，便会导致该错误信息。

- MySQL server has gone away

>管理日志中出现该错误，说明在atlas在获取后台MySQL的状态时失败，失败返回的errmsg为：MySQL server has gone away。

- CON_STATE_SEND_QUERY_RESULT send query result to client failed

>sql日志中出现上述错误，主要原因：客户端将连接断开。（断开原因可能是客户端连接超时断开的等等

- DBProxy `sql日志`中记录了慢查询（`[Slow Query]`），但在mysql慢查询日志中却没有发现该慢查询日志

>原因主要因为：DBProxy本身延迟（可能是互斥造成，也可能对sql处理等造成），从而该查询在MySQL上执行没有被认为是慢查询，而在Atlas中认为是慢查询。

### Q22: DBProxy和MHA能否一起使用，需要配置什么？

DBProxy 可以和MHA一起用，需要配置MHA的switchover和failover脚本里面提升新主库的时候增加上对DBProxy的操作，对DBProxy的主要操作可以参考 [backend管理]( https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.3.5)。

### Q23: 能否从管理接口看到SQL是发到哪个实例？

目前还不能从管理接口看到某条SQL的路由信息 ，在日志里是可以看到每一条SQL的路由情况。

### Q24: sysbench压 DBProxy会报错，和DBProxy里面什么限制有关系么？

DBProxy的限制参考 [SQL语句支持限制](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.2)。

### Q25: 连接DBProxy发送SQL语句时，报错：I have no server backend，是什么导致的？

登陆管理端口查看backend的状态，执行的语句：select * from backends;

如果backend的状态均为非UP状态，则检查当前连接DBProxy的用户名密码是否能直连上MySQL，网络是否通畅等；

如果backend的状态为UP状态，且只有部分语句报错，可能由于网络的抖动引起的。

### Q26: DBProxy配置的数据库是否可以跨机房？

网络可用就可以，跨机房可能会有网络延迟。

### Q27: select  * from backends 看到的threads_running都是0，什么原因导致的？

可能由于在配置文件中没有配置backend-monitor-pwds，或该参数配置有误。

### Q28: 配置的密码，哪些是需要加密的？

配置文件中：pwds和backend-monitor-pwds的密码需要加密。

### Q29: 连接DBProxy的proxy端口的密码是加密前还是加密后的？

加密前的，连接DBProxy和直连MySQL一样。

### Q30: 监控账户密码 一般给予什么样的权限？

监控账户最多会执行语句：SHOW STATUS LIKE 'Threads_running';，配置最基本的权限就可以。

### Q31: 编译 GLIB2.0过程中出现 No package 'glib-2.0' found 如何解决?
vim ~/.bashrc
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
source ~/.bashrc

### Q32: github上的代码是美团线上运行的么？是否是最新的代码？
	
是的，最新的。

### Q33: DBProxy 0.2版本分表策略只有一种吗？

目前版本只支持简单的按 分表键 hash/取模 这一种分表策略，后续版本会支持range。

### Q34: 有专门连接DBProxy的驱动吗？

使用DBProxy和使用MySQL几乎没区别，业务透明，用什么驱动连接MySQL就用什么连接DBProxy。

### Q35: DBproxy必须要跟mysql安装在同一台机器上么？

DBProxy的部署和MySQL的部署位置没有必然联系，可以同一台也可以分别部署。

### Q36: DBProxy源码编译时，为什么需要 --with-mysql选项？

DBProxy用到了MySQL C API，需要相关的库和头文件。

### Q37: DBProxy本身高可用?

DBProxy本身高可用通过lvs、haproxy4层负载均衡来保证。backends(MySQL)的master节点的高可用通过mha保证；slave节点的高可用通过DBProxy保证。

### Q38: 有相关DBProxy结合mha的文档吗?

mha相关文档还没有整理。

### Q39: DBProxy在美团中如何实现高可用的？

DNS——MGW(LVS)——DBProxy——MySQL 再配合 mha。

### Q40: DBProxy不支持空密码连接么？

不支持，安全角度考虑，生产环境 不会配置空MySQL 密码。

### Q41: DBProxy支持MyBatis吗？

支持，配置方法也和直连MySQL一样，DBProxy对上游应用是透明的。

### Q42: 目前支持分表的，全局唯一ID吗？

不支持，目前分表只是按照指定的分表键做hash之后表名替换。

### Q43: 支持官网版本的mysql 吗？还是只能用percona?

只适配过Percona，生产环境用的也是Percona，不过有些公司也使用了官方MySQL。因为DBProxy与MySQL交互只与MySQL协议有关，所以使用官方MySQL也应该没问题。但是保证DBProxy所在机器安装Percona。

### Q44: set names utf8 collate utf8_unicode_ci;设置该字符集报错

https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.2.1.5

### Q45: pdo客户端连接执行报错： 1105 Proxy Warning - Syntax Forbidden Prepare:XXXX ？

https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.2.2.4

### Q46: backend-max-thread-running＝64,如果数据库的连接大于这个参数会出现什么问题？

首先backend-max-thread-running的限制跟你说的连接数可能不是一回事，我理解你说的可能是MySQL 端 max_connections 变量，DBProxy backend-max-thread-running参数对应MySQL Threads_running变量。在DBProxy上，该功能可以通过设置选择开启或不开启。如果开启该功能，在路由某一后台MySQL时，发现当前的MySQL Threads_running过高，则会找其他的后端MySQL，直到找到一个可用的MySQL，将SQL路由给该后端MySQL。

### Q47: DBPrxoy 执行：show processlist 显示出来的backend是实时的么？

是。

### Q48: backend-max-thread-running 作用？

控制MySQL并发量，降低MySQL的压力等，也是为了对MySQL进行保护。

### Q49: 读写分离是不是只用配置backend就可以了?

是的。

### Q50: 哪些语句DBProxy会发往主库？

一条语句满足以下条件会路由到主库：1 写/更新 操作 2 事务中 3 通过注释强制走主库 4 其他从库不可用 等。