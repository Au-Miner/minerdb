# MinerDB

## What is MinerDB
MinerDB 是一个轻量级、简单可靠的基于raft的分布式kv存储引擎，支持分布式事务、容器化部署、注册中心以及磁盘存储等功能。


## Status
目前MinerDB建议仅供学习和个人项目使用，不建议在生产环境中使用

## Design Overview
![MinerDB.png](MinerDB.png)

## Main Module
MinerDB目前主要模块有MinerRPC、Min、Raft、MinerDB组成

### MinerRPC
MinerRPC 是一个基于 Socket+Zookeeper 实现的纯GO语言 RPC 框架。
MinerRPC支持动态代理、网络传输、序列化、服务注册、服务发现、服务调用等功能。

MinerDB使用MinerRPC作为集群内部通信框架，使用zookeeper作为注册中心，并支持容器动态纵向扩容

更多信息请见 [minerrpc](https://github.com/Au-Miner/minerrpc)

### Min
Min是一个轻量级、简化版本的Gin轮子，Min专注于性能和简单性，实现了Gin的大部分基础功能，
包括路由、中间件、请求/响应处理、错误处理、路由组等，用于快速搭建HTTP服务

MinerDB使用Min来对外提供HTTP服务

更多信息请见 [min](https://github.com/Au-Miner/min)

### Raft
MinerDB使用Raft共识算法进行数据复制和一致性保证，目前写请求支持线性一致性，读请求支持顺序一致性
1. 读请求：可向集群任一节点发起读请求并执行
2. 写请求：向Leader节点发起写请求，Leader节点将写请求广播给集群中的其他节点，当大多数节点写入成功后，Leader节点将写请求提交

### MinerDB
MinerDB分为Execution层和Storage层
1. Execution层负责提供上层算子和单机存储引擎对外接口
2. Storage层基于WAL+Bitcask实现数据持久化存储，基于BTree实现索引，
   支持读写锁的管理，支持事务acid，可重复读隔离级别，对写操作效率较高
    1. WAL：在事务执行期间将日志信息存储在WAL中，在Commit阶段更新到内存中的Bitcask里
    2. 原子性：基于严格两阶段锁协议，仅支持在Commit阶段进行数据更新
    3. 隔离性：基于读写锁实现
    4. 持久性：通过设置Bitcask数据内存写入磁盘间隔时间来实现持久型
    5. 可重复读：事务的写操作只会在Commit阶段更新到内存中的Bitcask里，任何其他读操作都不会受到写操作的影响
    6. GC：定期对磁盘文件进行merge，并写入Hintfile来存储索引信息

## Future Plan
- [ ] 死锁检测
- [ ] 提高raft稳定性
- [ ] 实现更多索引结构
- [ ] 支持更多事务隔离级别
- [ ] 支持sql，实现数据库前端（Parser、Planner、Optimizer）
- [ ] 支持更多算子
- [ ] 支持向前和向后的迭代器

## Getting Started
支持容器化docker-compose快速部署
```
docker-compose up --build -d
```

## Thanks
MinerDB受到了etcd、oceanbase、nubedb、rosedb、waterdb的启发，十分感谢🙏