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

# MySQL 多实例 → 单张 Paimon 表：当前需求与实现

更新：2026-10-20。本文替代初版“表同步＋整库同步”的范围定义。

## 1. 范围

用户明确要求只做 `mysql_sync_table`。一个 Flink 作业连接多个独立 MySQL 实例，
将启动时匹配的多个物理分片写入一张显式指定的 Paimon 表。不同实例允许使用各自的
地址、凭据、库表正则、时区、Source 并行度和启动位点。

不扩展 `mysql_sync_database`。该 Action 及 `SyncDatabaseActionBase` 恢复官方 1.3
的单实例实现；构造入口和 CLI 对 `source.*` / `multi-source.*` 明确报错，防止
传入多个实例后被静默当成单实例执行。旧单实例调用保留。

本次删除具名整库的分组、动态目标建表、分离/合并 sink 模式扩展，以及 PR #3 初版
加入通用 EventParser、动态建表 ProcessFunction 的 schema 校验钩子。
不通过继续修改整库路径来修复 Codex 关于该路径的 P1/P2。

## 2. 参数协议

使用 `--mysql_conf source.<id>.<option>=<value>` 声明实例，未带前缀的配置作为公共默认值。
实例覆盖公共值。`hostname` 必须逐实例声明，不能与公共 hostname 混用。
每个实例需要独立 `server-id` 或范围，容量不得小于其读取并行度。
重复具名参数、两种 mysql_conf 拼写混用、同一配置端点的 server-id 范围重叠均拒绝。
特定位点配置必须逐实例声明；校验错误不输出密码。

```bash
flink run -p 2 paimon-flink-action-1.3-SNAPSHOT.jar mysql_sync_table \
  --warehouse hdfs:///warehouse/paimon --database ods --table orders \
  --mysql_conf username=cdc_reader --mysql_conf "password=${MYSQL_CDC_PASSWORD}" \
  --mysql_conf 'database-name=trade_.*' --mysql_conf 'table-name=orders_.*' \
  --mysql_conf server-time-zone=Asia/Shanghai \
  --mysql_conf source.shard_a.hostname=mysql-a.internal \
  --mysql_conf source.shard_a.server-id=5400-5403 \
  --mysql_conf source.shard_a.scan.parallelism=1 \
  --mysql_conf source.shard_b.hostname=mysql-b.internal \
  --mysql_conf source.shard_b.server-id=5500-5503 \
  --mysql_conf source.shard_b.scan.parallelism=2
```

## 3. 主键与副作用

默认 `multi-source.key-mode=isolated`，目标主键为
`(source_id, database_name, table_name, <业务主键>)`，三个身份列使用非空 STRING。
源主键在全部分片之间全局唯一时，可以显式选择 `global`，保留业务主键而不自动添加身份列。
`global` 不提供跨实例事务顺序或并发冲突仲裁。

启动顺序为：校验配置 → 发现全部实例和分片 → 合并 schema → 计算列和元数据 →
校验物理主键保留关系 → 检查实际目标主键/分区/schema → 创建目标或修改允许的表选项。
显式 `--primary_keys` 允许保留全部物理主键的超集，但不得丢掉源主键。
失败前不创建错误目标表，也不修改已有目标选项。

`source_id` 是稳定业务标识，不是连接字符串；修改它会改变来源身份和 Source UID。
已有仅以 id 为主键的目标表不能直接切到隔离模式。

## 4. 物理分片与运行期校验

库表正则仅用于启动时发现。每个 Source 的实际 tableList 由该实例已校验的物理库表名
逐项转义后排序生成，不再运行期发现新表。一个新物理分片要在重新提交任务时经过发现和校验。
新增分片/实例、变更来源集合、旧作业状态迁移都需要单独验证恢复行为，不默认保证直接恢复。

`MySqlTableSchemaValidator` 保存每个物理表的主键列名和类型，内容可 Java 序列化；
不将不可序列化的 `Schema` 对象放入算子闭包。普通列信息无需保留在约束对象中。

`MySqlRecordParser` 在输出事件前检查物理表身份；在输出 schema 事件前检查：
主键未替换、未删除，主键类型未变化，schema history 完整且可解析。
校验在 Source 对应的 Parser 内完成，早于 union、主键路由和 sink，避免先输出数据再校验。
普通加列和非主键类型拓宽仍使用既有 schema evolution。旧单实例 Parser 未启用此约束，行为不变。

## 5. 源码位置

| 位置 | 职责 |
| --- | --- |
| `mysql/MySqlSourceConfigs.java` | 具名配置、table-name 必填、DB API/CLI 拒绝具名输入。 |
| `mysql/MySqlSyncTableAction.java` | 全实例发现、无副作用预检、固定物理表集、独立 Source 与 Parser。 |
| `mysql/MySqlTableSchemaValidator.java` | 物理主键契约、转义 tableList、启动目标主键保留检查。 |
| `mysql/MySqlRecordParser.java` | 具名单表运行期 fail-fast；普通单源不启用。 |
| `mysql/MySqlMultiSourceSupport.java` | 来源元数据、目标 schema 校验和单一目标的 typed-key 路由。 |
| `SynchronizationActionBase.java` | 保留 table Action 所需输入构建扩展点。 |
| `SyncDatabaseActionBase.java`、`sink/cdc` 通用解析类 | 撤销本功能的整库/动态建表改动。 |

上述路径均位于 `paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/`。

## 6. 测试和合并门槛

保留具名单表配置、工厂、schema、Parser、路由测试，以及双 MySQL 表同步和 savepoint 测试。
删除新增整库专属用例。新增主键保留、运行期主键替换/删除/类型变更、固定分片过滤、
序列化、DB 拒绝具名输入等回归，并在实际 Action/Parser 测试中检查失败前没有目标表/输出。

独立 JVM 的 guard 测试不能代替完整 Maven 编译、JUnit、Checkstyle/Spotless 和 MySQL/Flink
端到端。当前执行证据见同目录验证报告。Codex 必须完成针对最新 head SHA 的复审，
无未处理结论才能认为 review 通过；没有 Check Run、只有旧版本测试或仅有 mergeable=true 均不算。
本 PR 保持打开，不自动合并，不修改分支保护。
