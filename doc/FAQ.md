## DBProxy FAQ


> 该文档中的FAQ主要整理自DBProxy用户QQ交流群(367199679)大家的交流内容，将共性的问题进行整理、积累，方便大家回顾和学习。持续更新中...


### Q1：CentOS默认源中glib2的版本是2.28.8-4.el6，会导致DBProxy make报错，如何获取正确glib2版本？

[glib-2.4.2.0.tar.xz download](http://pkgs.fedoraproject.org/repo/pkgs/mingw-glib2/glib-2.42.0.tar.xz/71af99768063ac24033ac738e2832740/)


### Q2: DBProxy的后端连接是怎么复用的？代码中没有看到change user这个过程。

DBProxy中，将可复用的连接放回连接池，在连接池中，是按照用户名来管理连接的。如果user1 新建立的连接，DBPRoxy会去分给它的backend的连接池中，找到user1的连接，如果有，则分配，没有则新建立连接。

示意图可以参考  [开发手册](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/PROGRAMMING_GUIDE.md)最后的那张图 

### Q3: DBProxy是如何控制最大连接数的？是否针对不同用户设置最大连接数?

DBProxy中连接池中连接的数量目前还没有限制；目前也没有针对某一用户来统计连接数量，针对某一用户连接数来限制；DBProxy只统计了连接DBProxy的总连接，不区分用户，根据统计的总连接，可以动态设置最大允许连接DBproxy的连接数量。

### Q4: DBProxy 可以在ubuntu server上运行吗？

目前DBProxy 只在CentOS6.5上进行过适配。

需要的环境和依赖库参考[快速入门手册](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/QUICK_START.md)

### Q5: DBProxy的文档在哪里?

详见[README_ZH](https://github.com/Meituan-Dianping/DBProxy/blob/master/README_ZH.md)

### Q6: DBProxy是否可以立即在生产环境使用？

目前开源的DBProxy与美团点评内部大规模生产环境使用的版本一致。

### Q7: DBProxy与MyCat有何异同？

我们没有使用过MyCat产品的经验，所以不好比较，根据网上资料，可能主要功能类似，各有个特点吧。

### Q8: DBProxy对mysql的版本有限制么？

目前测试过mysql5.5 和 mysql5.6。

**需要注意：**DBproxy 配置文件中需要配置下后端mysql的版本，默认5.5 。例如，使用mysql是5.6时候，配置文件中配置参数 mysql-version=5.6。

### Q9: 美团已经把DBProxy 的所有代码都开源了吗？

DBProxy 完全开源了，美团点评 目前内部使用的版本和github上的版本一致。我们后续的所有维护、开发都会直接在github上进行。

### Q10: DBProxy自己管理与MySQL的连接池么？以及如何管理？
是自己管理的。

DBProxy的每个处理线程（一个处理线程可能同时处理多个客户端连接）对于每个后台MySQL都有一个连接池；处理线程首先从连接池中获取后台db连接，如果连接池不存在连接时，则新建后台db连接；使用完成后（语句执行完成后，当前连接不在事务中，而且当前连接没有last_insert_id、found_rows等上下文信息），会将连接放入连接池；连接池中的连接空闲超过一定时间（可由db-connection-idle-timeout配置，默认为1小时）后，DBProxy会自动关闭该链接。

### Q11: DBProxy的连接池如何处理session级变量？
DBProxy会在每个客户端连接记录当前连接设置的session变量状态，同时在每个后台db连接上也会记录该连接设置的session变量状态；当后台db连接被重用时，根据客户端连接和后台db连接的session变量状态，重新设置后台db连接的session变量状态。

### Q12: DBProxy的连接池如何处理事务？
首先客户端连接的整个事务由同一个后台db连接来处理，其次如果客户端连接关闭时仍然在事务中，不会将连接放入连接池而是关闭对应的后台db连接，由MySQL回滚该事务。

### Q13: DBProxy和主mysql放一台物理机对性能影响大吗?

DBProxy消耗资源不大，性能影响不大。

### Q14: DBProxy 前面加个lvs这种方式，程序支持没有问题吧？

没有问题，美团点评也使用lvs。

### Q15: DBProxy 支持分库分表吗？还是只支持分表？

目前开源的版本只支持分表，分库分表版本在内测，稳定后会立即开源。

### Q14: DBProxy 主要是什么语言编写的？

主要是C，有一小部分Lua。

### Q16: DBProxy的负载均衡算法是什么？

现阶段：   
 
- 权重一样，简单的从库轮询（RR）；    

- 权重不同，按权重比例轮询（例如：只有两个可用的slave, slave 1:weight 4  , slave 2: weight 1, 每5次4次发送查询语句到slave1,1次 发送查询语句到slave2）；   
 
- 配置了tag，会根据配置的tag发送到指定的从库（tag详细参考 [从库流量配置](https://github.com/Meituan-Dianping/DBProxy/blob/master/doc/USER_GUIDE.md#3.3.7.2)）；

- 支持threadrunning功能，进行过载保护（DBProxy会周期获取MySQL的实际threadrunning，根据DBProxy 上配置的threadrunning来选择可用的从库）。











