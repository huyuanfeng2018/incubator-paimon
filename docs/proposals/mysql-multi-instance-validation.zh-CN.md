<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# MySQL 多实例单表同步：验证记录

## 本轮范围：2026-10-20

用户将需求收敛为 `mysql_sync_table`。具名 `mysql_sync_database`、整库分组/动态建表/
通用 sink 校验扩展和整库专属测试均撤回。原有单实例整库 Action 保留，仅对具名多实例配置报错。

## 已执行

1. 使用原交付包中的 Paimon Flink 1.20 JAR 作为真实 Paimon 依赖，执行
   `javac -proc:none --release 8` 编译新的 `MySqlTableSchemaValidator`。
   提交源码原样编译，不删注解、不替换业务依赖为 stub。
2. 独立 JVM 回归程序 **21 passed / 0 failed**：有效物理分片、普通加列、主键替换、主键删除、
   主键类型变化、主键字段缺失、未知库/表、大小写、两种模式目标保留/丢弃源主键、
   主键超集、合并后空主键、literal tableList、Java 序列化、外部 Map 修改隔离。
   首轮真实序列化测试发现 `Schema` 不可序列化；实现改为仅保存可序列化主键契约后重新执行，全通过。
3. JDK 编译器的 Java 8 语法解析检查当前工作副本中的 Java 文件通过。
   此项仅解析语法，不做类型检查、不代表模块编译。
4. `git diff --check` 和源码恢复 blob 核对用于防止撤回时夹带无关改动。

本轮 JVM 为 Java 21；guard 生成 Java 8 字节码。使用的 Paimon JAR 来自此前交付归档。
当前运行环境没有 Maven、Flink/JUnit 完整依赖，外网 DNS 不可用。

## 已编写但未在本机执行

- `MySqlTableSchemaValidatorTest`：固定物理表与主键契约的 JUnit 回归。
- `MySqlMultiSourceParserTest`：真实 Parser 在 collector 输出前拒绝主键 ALTER、DROP 和缺失 history。
- `MySqlMultiSourceTableTest`：显式主键丢弃源主键时，targetExists=false。
- `MySqlMultiSourceFactoryTest`：API、两种 CLI 拼写及 fallback 隐藏输入均拒绝具名整库。
- 原有单表配置/路由和两套 MySQL 的表同步、savepoint 回归保留。

没有把独立 JVM 的 21 项检查当作 JUnit 执行，也没有把旧实现的 181 项测试结果记到本轮。
完整 Maven 编译、JUnit、Checkstyle/Spotless、双实例端到端与 checkpoint 恢复仍需有完整依赖的环境执行。

```bash
mvn -B -ntp -Pflink1 -pl paimon-flink/paimon-flink-cdc -am \
  -Dflink.forkCount=1 \
  -Dtest=MySqlTableSchemaValidatorTest,MySqlMultiSourceTableTest,MySqlMultiSourceParserTest,MySqlMultiSourceRoutingTest,MySqlMultiSourceFactoryTest,MySqlSourceConfigsTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

## 历史证据与当前门槛

PR #2 原版本记录的 181 项及 PR #3 初版记录的 25 项独立 JVM 检查属于旧源码；
包含整库路径，不能证明本次收敛后的源码已经完整验证。

Codex 状态以 PR #3 最新 head SHA 的 review summary 和线程为准。
本报告不预先宣称 Codex 通过或 GitHub CI 通过。最新 head 的 review 未结束或有未处理意见时不得合并。
