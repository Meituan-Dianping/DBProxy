### 概述   
DBProxy工程中包含了大量的测试用例，用来对DBProxy进行自动化测试。测试用例所在的文件路径为：  

```
./dbproxy/tests/suite
```

该路径下的各个文件夹测试内容与DBProxy功能模块的对应关系：

<table>
<tr>
	<th>文件夹名称</th>
	<th>测试内容</th>
</tr>
<tr>
	<td>mysql-test</td>
	<td>兼容的SQL语句的测例</td>
</tr>
<tr>
	<td>trx-test</td>
	<td>事务相关的测例</td>
</tr>
<tr>
	<td>basic_manager</td>
	<td>DBProxy基本操作模块的测例</td>
</tr>
<tr>
	<td>user_manager</td>
	<td>用户管理模块的测例</td>
</tr>
<tr>
	<td>connection_manager</td>
	<td>连接管理模块的测例</td>
</tr>
<tr>
	<td>sharding_manager</td>
	<td>分表管理模块的测例</td>
</tr>
<tr>
	<td>backend_manager</td>
	<td>backend管理模块的测例</td>
</tr>
<tr>
	<td>log_manager</td>
	<td>日志管理模块的测例</td>
</tr>
<tr>
	<td>flowcontrol_manager</td>
	<td>流量管理模块的测例</td>
</tr>
<tr>
	<td>statistics_manager</td>
	<td>统计信息管理模块的测例</td>
</tr>
<tr>
	<td>monitor_manager</td>
	<td>监控管理模块的测例</td>
</tr>
<tr>
	<td>mutable_test</td>
	<td>其他测例</td>
</tr>
</table>

### 运行方法
- 配置

修改./dbproxy/tests/suits/test.configure文件中的对应配置，该文件的配置信息会生成DBProxy的配置文件

- 运行
进入某个测例文件夹（以basic_manager为例）,然后执行语句：

```
cd ./dbproxy/tests/suits/basic_manager
LUA_CPATH=/usr/local/mysql-proxy/lib/mysql-proxy/lua/?.so lua ./../run-tests.lua ./
```

### 添加测例

您可以仿照如下方法进行添加新的测例：

```
connect (conn0, 127.0.0.1, dbproxy_test, '123456', 'test', 6018);  #建立连接
connection conn0; #使用该连接
#编写测试语句
show databases;
use dbproxy_test;
select * from test;
```

### 注意  

1. 每个测例文件夹中都会有r文件夹和t文件夹。r文件夹放置运行结果;t文件夹放置测试用例。

2. 当运行测例时选择“normal”模式时，是将DBProxy运行的结果与result文件进行对比。目前github上提供的result文件内容可能与您实际运行的结果不一致，您需要对result文件进行相应的修改，请主要关注result文件中以下内容，根据您实际情况修改：

```
select * from backends 执行后的结果
show variables         执行后的结果
select version         执行后的结果


```




