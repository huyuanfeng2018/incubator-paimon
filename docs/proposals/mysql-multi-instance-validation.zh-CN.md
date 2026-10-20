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

# MySQL 多实例 CDC：验证与交付说明

## 1. 结论

本次实现基于 Apache Paimon `release-1.3` 的提交 `5c59e6cb01ed0b29563371f56e14fcade4597a2e`。新增的 125 个单元测试、3 个真实双实例端到端测试和 53 个现有回归测试均已通过，合计 181 个不同测试用例，零失败、零错误、零跳过。

最后一轮包含全部 125 个新增单元测试及 3 个端到端测试，共 128 例，执行期间保留 Checkstyle 与 Spotless 门禁，结果 `BUILD SUCCESS`。现有 53 例回归在前一轮独立记录，不把重复执行的新测试重复计数。真实双实例端到端测试合计耗时 50.091 秒；这不是吞吐性能基准。

验证日期为 2026-10-19。完整需求、配置协议、实现取舍和上线边界见同目录 `mysql-multi-instance-cdc-prd.zh-CN.md`。

## 2. 实际环境

| 项目 | 实际值 |
| --- | --- |
| Paimon | 官方 `release-1.3`，项目版本 `1.3-SNAPSHOT` |
| Flink | `1.20.1` |
| Flink CDC | `3.1.1`，使用该分支 CDC 模块原有依赖 |
| Maven / Java | Maven `3.9.9`，OpenJDK `17.0.20`；Java 编译目标沿用项目设置 |
| MySQL | 两个真实独立 MySQL `8.0.46` 进程，各有独立 datadir、server UUID 和 binlog |
| 连接 | 实例 A `127.0.0.1:33061`，实例 B `127.0.0.1:33062` |
| MySQL 日志设置 | `binlog_format=ROW`、`binlog_row_image=FULL`、`gtid_mode=ON` |
| 时区 | 两个真实实例使用 UTC；不同实例时区差异另由 parser 单元测试覆盖 |
| Flink 测试运行 | 项目已有 MiniCluster，作业并行度 2，两个 Source 并行度分别为 1、2 |
| Paimon 测试目标 | 临时文件系统 catalog/warehouse，读取实际提交后的 Paimon 数据断言 |
| 恢复 | 真正提交带 `SavepointRestoreSettings` 的 StreamGraph，不只修改未执行的 JobGraph |

本环境没有 Docker，因此使用两个本机 MySQL 进程执行了端到端测试。测试类也保留了项目现有 MySQL Testcontainers 的启动方式；本次没有运行 Docker 分支。实例由测试专用账户创建随机命名数据库，测试结束删除自己的测试数据库。

### Java 17 测试运行依赖

首次运行 catalog 测试时，出现 `org.apache.hadoop.shaded.com.ctc.wstx.io.InputBootstrapper` 缺失。定位结果是 Java 17 激活 `orc-shims 1.9.4` 的 `java17` profile，引入 `hadoop-client-api 3.3.5`，而配套 `hadoop-client-runtime` 未进入现有测试 classpath。

本次通过 Surefire 的 `maven.test.additionalClasspath` 参数补入 Maven Central 官方 `hadoop-client-runtime-3.3.5.jar`，核对公开校验和后重跑全部相关测试。没有为此修改项目 POM，也没有关闭或跳过测试来规避该问题。此参数用于本次 Java 17 测试环境，不是新增 Action 配置。

## 3. 新增单元测试结果

| 测试类 | 用例数 | 验证重点 | 结果 |
| --- | ---: | --- | --- |
| `MySqlSourceConfigsTest` | 75 | 旧配置保留、公共默认/实例覆盖、稳定 ID 排序、非法配置、每实例并行度、server-id 范围与真实 CDC 兼容、原始 Map 不变 | 全部通过 |
| `MySqlMultiSourceFactoryTest` | 10 | 两个真实 Factory 入口、重复具名参数、CLI 两种拼写、混用防丢实例、legacy 行为 | 全部通过 |
| `MySqlMultiSourceParserTest` | 8 | before/after 事件、来源身份、各实例时区、大小写冲突、DDL/DML 列冲突、无主键状态按数据库隔离、运行期过滤及序列化 | 全部通过 |
| `MySqlSyncDatabaseMultiSourceTest` | 11 | 跨实例 schema 合并、独立排除、目标分组/table_mapping、既有目标主键预检、整组忽略持续生效、新目标主键、启动失败无目标表写入 | 全部通过 |
| `MySqlMultiSourceRoutingTest` | 14 | 目标表与主键路由、payload/RowKind 无关、大小写、时间与数值表示等价、二进制内容、未知目标稳定路由、序列化、非主键列演进 | 全部通过 |
| `MySqlMultiSourceTableTest` | 7 | 两个真实 Source 的 JobGraph、不同并行度、稳定 UID、隔离/global 模式、已有目标 PK 校验、源 schema 合并/冲突、旧单源图兼容 | 全部通过 |
| **合计** | **125** | | **0 失败 / 0 错误 / 0 跳过** |

对已有目标分区键也实现了显式一致性校验；上述新增 table 测试没有单列分区键失配用例，不能把它理解成另一个已运行的独立验收项。

## 4. 真实双实例端到端结果

| 用例 | 实际执行和断言 | 结果 |
| --- | --- | --- |
| `testTableSnapshotDmlAndIndependentSchemaChanges` | 两个实例拥有同名数据库、表和 `id=1`；初始快照保留两条；双方 INSERT/UPDATE/DELETE；各实例独立加列与普通列 INT→BIGINT 拓宽；每个实例对同一行连续更新 150 次，校验最终值；分别修改主键，校验旧键删除与另一实例数据隔离 | 通过 |
| `testTableSavepointRestoresSourceIdsAfterConfigurationReordering` | 两实例进度写入 Paimon后执行真实 `stopWithSavepoint`；停机期间更新、插入以及删除实例 B 旧行；使用反序 LinkedHashMap 重建并恢复作业；确认离线变更及恢复后新增变更完整到达，旧 B 行被删除 | 通过 |
| `testCombinedDatabaseDiscoversTablesIndependently` | 先从实例 A 同步 payments，后在实例 B 新增同名表并汇入已有目标；再在双方新增 invoices，检查动态目标建表及三列来源主键；两个实例的同名 ID 均保留 | 通过 |

恢复用例特意包含停机期间 DELETE：若配置了 savepoint 却实际重新全量扫描，目标中已写入的旧行不会被重新快照删除，这个断言能识别这种假恢复。

整库动态新表测试使用该分支现有配置路径，`scan.newly-added-table.enabled=false` 时运行中新表仍被正确捕获。本次没有把该 flag 的其他组合、所有 startup mode 或所有 MySQL 版本列为已验证能力。

## 5. 现有回归结果

| 现有测试类 | 用例数 |
| --- | ---: |
| `UpdatedDataFieldsProcessFunctionBaseTest` | 23 |
| `TableNameConverterTest` | 2 |
| `CdcRecordKeyAndBucketExtractorTest` | 3 |
| `CdcRecordChannelComputerTest` | 2 |
| `CdcMultiplexRecordChannelComputerTest` | 2 |
| `FlinkCdcMultiTableSinkTest` | 1 |
| `ComputedColumnUtilsTest` | 2 |
| `SyncDatabaseActionBaseTest` | 3 |
| `org.apache.paimon.flink.action.cdc.SchemaEvolutionTest` | 1 |
| `DebeziumEventTest` | 1 |
| `org.apache.paimon.table.SchemaEvolutionTest`（core） | 13 |
| **合计** | **53，全部通过** |

这些测试针对本次复用或增加扩展点的 schema、表名映射、CDC 分区和公共 Action 路径。没有声称跑过整个 Paimon 仓库所有测试或其他连接器的完整 Docker 集成套件。

## 6. 复现命令

命令从应用补丁后的 Paimon 仓库根目录执行。先按项目原有构建方式准备上游模块及 codegen 资源：

```bash
mvn -B -ntp -Pflink1 \
  -pl paimon-flink/paimon-flink-common -am \
  -DskipTests package
```

准备两个 MySQL 测试实例后，执行新增单元测试与端到端测试。测试类默认使用 `paimonuser` / `paimonpw` 测试账户，可以通过 `paimon.mysql.user` / `paimon.mysql.password` 系统属性覆盖。账户需具备测试数据库创建/删除和 CDC binlog 读取权限。

```bash
mvn -B -ntp -Pflink1 \
  -pl paimon-flink/paimon-flink-cdc -am \
  -Dflink.forkCount=1 \
  -Dtest=MySqlSourceConfigsTest,MySqlMultiSourceFactoryTest,MySqlMultiSourceParserTest,MySqlSyncDatabaseMultiSourceTest,MySqlMultiSourceRoutingTest,MySqlMultiSourceTableTest,MySqlMultiSourceITCase \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dpaimon.mysql-a.port=33061 \
  -Dpaimon.mysql-b.port=33062 \
  -Dmaven.test.additionalClasspath=/absolute/path/hadoop-client-runtime-3.3.5.jar \
  test
```

如果使用 Docker/MySqlContainer，移除两个外部端口参数即可；本次执行的是上面显式端口的真实双进程模式。已有可用且兼容的 Java/Hadoop 测试 classpath 时，可按环境需要省略 `additionalClasspath` 参数。

相关回归的测试选择为：

```text
SyncDatabaseActionBaseTest,SchemaEvolutionTest,TableNameConverterTest,ComputedColumnUtilsTest,DebeziumEventTest,CdcRecordChannelComputerTest,CdcMultiplexRecordChannelComputerTest,CdcRecordKeyAndBucketExtractorTest,UpdatedDataFieldsProcessFunctionBaseTest,FlinkCdcMultiTableSinkTest
```

打包命令：

```bash
mvn -B -ntp -Pflink1 \
  -pl paimon-flink/paimon-flink-1.20,paimon-flink/paimon-flink-action -am \
  -DskipTests package
```

此处 `-DskipTests` 用于已完成测试后的单独打包；最终测试命令没有使用它。构建与最终测试均保留项目原有 Checkstyle/Spotless 校验。

## 7. 构建产物

标准打包成功，共 25 个 reactor 模块，耗时 2 分 52 秒；未跳过 Checkstyle/Spotless。产物内容检查确认 Flink 1.20 主 JAR 包含新增配置/路由类及改动的 Parser、两个 Action 和两个 Factory，ServiceLoader 注册完整。随后从运行 classpath 中剔除所有 reactor `target/classes`，使用打包 JAR 实际执行两个 Action 的 `--help`，均加载到新版具名多实例帮助。

| 产物 | 字节数 | SHA-256 |
| --- | ---: | --- |
| `paimon-flink-1.20-1.3-SNAPSHOT.jar` | 52,947,243 | `fa55a934a5161d487c16d946256d50315529ed5f85faee0cd3168afde3ae5a7e` |
| `paimon-flink-action-1.3-SNAPSHOT.jar` | 11,575 | `8a8c0882a18721ac3dbe51d32f8f42dffc396eb976527d0309fd9cf328a5cb19` |

部署时，包含 CDC 实现的 Paimon Flink 1.20 主 JAR 与入口 Action JAR 应使用本次对应构建。Flink runtime、Flink CDC/MySQL JDBC 等依赖继续遵循项目原有部署方式。不要同时保留多个版本的 Paimon 主 JAR 导致旧类被优先加载。本实现没有验证内部改版的 Paimon 发行包或 CDC 3.6；这类环境应在自己的依赖树上重新编译和验证。

## 8. 已确认边界

- 默认隔离主键为来源三列加业务主键；选择 global 需要业务方确认所有参与物理表的全局主键语义。
- 具名整库模式要求 `merge_shards=true`，不支持数据库名式前后缀或整库计算列。
- 本次真实 savepoint 恢复覆盖表同步；未独立执行整库 savepoint、snapshot 中途 rescale、在线增删实例或旧单源状态迁移用例。
- 目标主键类型不能在线变更；普通列兼容演进沿用现有实现。计算主键必须具有确定性。
- 整库动态新分片映射已有目标时，必须预先确认业务主键兼容；不能把现有动态发现机制视为任意主键变更的保证。
- 作业构图时未知的新目标在本次运行内按整表稳定路由，路由阶段单表最多使用一个并行分区；这不是无限并行的新表读取承诺。
- 各来源共用作业 checkpoint，但没有跨 MySQL 实例的事务一致快照或全局事件顺序。故障与反压也会在一个作业中耦合。
- 从改变过路由的重新构图结果恢复时，使用 aligned checkpoint/savepoint；带旧通道状态的 unaligned checkpoint 迁移没有验证。
- catalog 建表不是跨目标表原子事务；启动发现/预检失败不会提前写目标表，通过预检后的 catalog 操作失败可能留下已建空表。

## 9. 审阅建议

先审阅 PRD 中的主键选择、目标映射和恢复边界，再看 `MySqlSourceConfigs`、两个 MySQL Action、`MySqlMultiSourceSupport` 和七个新增测试类。生产接入优先从新目标表试跑，确认实际业务数据键空间后再规划既有任务迁移。
