### 一、简介

DBProxy是由美团点评公司技术工程部DBA团队（北京）开发维护的一个基于MySQL协议的数据中间层。它在奇虎360公司开源的Atlas基础上，修改了部分bug，并且添加了很多特性。**目前DBProxy在美团点评广泛应用，包括美团支付、酒店旅游、外卖、团购等产品线，公司内部对DBProxy的开发全面转到github上，开源和内部使用保持一致**。目前只支持MySQL（Percona）5.5和5.6。

主要功能：

1. 读写分离
2. 从库负载均衡
3. IP过滤
4. 分表
5. DBA可平滑上下线DB
6. 自动摘除宕机的DB
7. 监控信息完备
8. SQL过滤
9. 从库流量配置

### 二、DBProxy相对于奇虎360公司开源Atlas的改进

1. 修改了部分bug并且新增了一些feature，详见[release notes](./doc/RELEASE_NOTES.md)
2. 提供了丰富的监控信息，大量参数可配置化并且支持动态修改
3. 对原有的诸如日志等模块进行了优化，性能提升明显
4. 开源版本即为目前美团点评内部使用版本，并将一直对源码及其文档进行维护


### 三、DBProxy详细说明

1. [DBProxy快速入门教程](./doc/QUICK_START.md)
2. [DBProxy用户使用手册](./doc/USER_GUIDE.md)
3. [DBProxy开发手册](./doc/PROGRAMMING_GUIDE.md)
4. [DBProxy架构和实践](./doc/THEORY_PRACTICES.md)
5. [DBProxy release notes](./doc/RELEASE_NOTES.md)
6. [DBProxy 测试手册](./doc/TEST_GUIDE.md)
7. [FAQ](./doc/FAQ.md)
8. [DBProxy开发规范](./doc/DEVELOPMENT_NORM.md)

### 四、DBProxy的需求及Bug反馈方式

如果用户在实际的应用场景中对DBProxy有新的功能需求，或者在使用DBProxy的过程中发现了bug，在github上进行交流或是PullRequest，后续将会建立DBProxy的公众号以及讨论组/群方便大家的使用和版本的维护。

![QQ](./doc/img/DBProxy用户交流群群二维码.png)

![QQ](./doc/img/DBProxy用户交流群2群二维码.png)

注意：QQ群1已经满员，请加入QQ群2(550320610)。
