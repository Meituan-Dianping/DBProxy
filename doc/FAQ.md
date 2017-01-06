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





