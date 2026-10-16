# docs 文档索引

> 本索引覆盖 `docs/` 目录下全部文档（含 `superpowers/plans`、`superpowers/specs` 与 `evidence` 证据材料），
> 每条给出中文标题、中文摘要与跳转链接。链接均相对本文件（`docs/INDEX.md`）。
>
> 更新日期：2026-10-15 · 共 66 篇文档 + 1 个图片资源（同日清理 11 篇已废弃/与实现不符的文档，见文末清理记录）

## 目录

- [总览与规划](#总览与规划)
- [审计与兼容性](#审计与兼容性)
- [性能优化](#性能优化)
- [并发与 session-as-tasks 专项](#并发与-session-as-tasks-专项)
- [会话交接 handoff](#会话交接-handoff)
- [superpowers plans 实施计划](#superpowers-plans-实施计划)
- [superpowers specs 设计规格与实施记录](#superpowers-specs-设计规格与实施记录)
- [性能证据 evidence](#性能证据-evidence)
- [其他资源](#其他资源)
- [推荐阅读顺序](#推荐阅读顺序)

---

## 总览与规划

| 文档 | 中文摘要 |
|------|----------|
| [《kcptun-rs 最终性能优化方案（定稿）》](./PERF_OPTIMIZATION_PLAN.md) | 性能工作总纲：目标与验收 KPI（吞吐 ≥ Go、时延 ≤ 1.10× Go）、已完成项与剩余路线、Rust 落点；P0/P1 主体已落地。 |
| [《kcptun-rs Rust 化优化方案（最终整合版）》](./RUST_IDIOMS_MIGRATION.md) | 系统梳理从 Go 直译带来的 Go-ism（类型系统 / 控制流 / 数据结构等检查清单），给出不破坏 wire 兼容前提下的 idiomatic Rust 迁移路线。 |
| [《R4 StreamInner 多锁合并 + common 二期抽取》](./R4_STREAM_INNER_AND_COMMON_EXTRACT.md) | SMUX `StreamInner` 多锁合并（R4）设计与 QPPPort / pipe / snmp_logger 二期抽取到 kcptun-common 的方案与验证门禁；两者均已实现。 |
| [《netclient：kcp-rs 移动端通用网络客户端设计方案》](./mobile-netclient-design.md) | 以 C ABI 库交付跨平台网络客户端（iOS / Android / Win / macOS / Linux），统一 `tcp://` / `kcp://` / `kcptun://` 拨号入口，面向 Unity 与 Godot 接入的设计稿。 |

## 审计与兼容性

| 文档 | 中文摘要 |
|------|----------|
| [《kcptun-rs 全面代码审计与性能优化报告》](./KCPTUN_RS_CODEBASE_AUDIT_REPORT.md) | 全 crate 审计：架构亮点（4-Phase Flush、双 runtime 抽象、零分配流水线）与隐患清单（SMUX stream map 锁争用、FEC 解码边界 panic 风险等）及改进建议。 |
| [《代码审查报告与生产级代码规范》](./CODE_AUDIT_AND_STANDARDS.md) | 9 个 crate、约 2 万行的逐文件审查（2026-08-02），F1–F16 发现项与生产规范；文内附 2026-08-03 起的实施状态更新（生产迁移、Builder 去重、thiserror 等）。 |
| [《Go kcptun CLI / 功能对齐审计与修复报告》](./GO_CLI_PARITY_AUDIT_2026-08-11.md) | 逐参数核对 client/server CLI、JSON 配置覆盖与运行行为；修复 autoexpire 语义、`--tcp --conn N`、remoteaddr 端口随机、closewait、framesize 等差异，并以 Go↔Rust 互操作验证。 |

## 性能优化

| 文档 | 中文摘要 |
|------|----------|
| [《kcp-rs 优化建议（2026-09-01）》](./kcp-rs-optimization-2026-09-01.md) | 证据门控的 kcp-rs 优化计划：死代码清理记录（逐项删除清单）与 P0/P1 建议（FEC 收发路径、共享时钟、`parse_fastack` 短路等）。 |
| [《kcptun-rs P99/P999 延迟性能分析与优化报告》](./P99_P999_PERFORMANCE_ANALYSIS_REPORT.md) | 定位尾延迟抖动来源（UDP syscall、runtime 调度开销），验证 fast retransmit 机制正确性，并给出降 P999 的优化方向。 |
| [《A0 留档：v1 性能改动逐条变更说明 + 做/砍清单》](./perf-v2-a0-change-breakdown.md) | `perf/opt-v2` 任务书的 A0 留档：v1 性能改动与 commit 映射（O1-P/O2/O3 等）及 O9–O17 各项做/砍结论对照表。 |

## 并发与 session-as-tasks 专项

| 文档 | 中文摘要 |
|------|----------|
| [《性能分析：保持 session-as-tasks，当前分支为什么比 main 慢》](./perf-analysis-session-task-2026-09-24.md) | 用实测打点证明：慢的根因是 Linux 内核 timer 粒度放大的跨任务唤醒（收→KCP→ACK 经 4 段调度），而非 KCP / 加密 / 发送本身。 |
| [《性能优化方案：保持 session-as-tasks 架构》](./perf-optimization-plan-session-task-listener.md) | 依据实测修正根因后的实施顺序：`feed_raw_single` 内联、`flush_acks_only`、flush loop 首醒即刷已落地，后续为 Linux 干净测量与 A/B 验证步骤。 |
| [《审查：feat/session-task-listener close / 资源回收》](./session-task-listener-close-lifecycle-review.md) | 对会话任务化 listener 关闭路径的审查报告：P1 发送可能永久挂起、P2/P3 资源回收与 `push` 检查问题及处置建议。 |
| [《核对（五）：验收测试与小清理补全》](./session-task-fix-verification-2026-09.md) | session-task listener 的验收补全记录：pending 清理/误删修复与 5 项原缺口验收测试的落地状态（含 5000 会话 sweep 测试）。 |

## 会话交接 handoff

| 文档 | 中文摘要 |
|------|----------|
| [《交接：会话存活 / 重连 / autoexpire（2026-09-15）》](./handoff-2026-09-15-session-liveness.md) | 修复三个线上问题：被替换会话改为「退休」制继续服务、单次 ICMP 不再杀会话、补齐 Go 的 keepalive 桶保护；含提交清单与门禁结果。 |
| [《交接：会话存活修复 + 真机路径诊断（2026-09-17）》](./handoff-2026-09-17-liveness-and-path.md) | 上一轮修复后的真机验证与路径诊断交接：本机测试环境、二进制构建/端口注意事项、集成测试与 bench 入口。 |
| [《交接：内核 socket 缓冲 vs. KCP 逻辑窗口（2026-09-18）》](./handoff-2026-09-18-kernel-buffers.md) | VPS 容器锁死 `net.core.*mem_max` 导致窗口不匹配：用 `SO_*BUFFFORCE` 把生效缓冲提到 4 MB，并记录 `Udp.InErrors` 才是丢包可见计数器的实测。 |
| [《交接：代理负载下的会话静默（TX token / flush 上界 / SMUX 窗口）（2026-09-20）》](./handoff-2026-09-20-tx-token-leak.md) | 复现「YouTube 刷新加载失败」并定位两个会话静默 bug 的修复过程、双端部署/回滚信息与后续路径问题。 |
| [《交接：feat/session-task-listener（2026-09-24）》](./handoff-session-task-listener-2026-09-24.md) | 把 KcpListener 从「自建线程 + 私有 runtime」改为调用方 runtime task 的分支交接：提交列表、未提交改动（`feed_raw_single` 等）与性能问题。 |

## superpowers plans 实施计划

| 文档 | 中文摘要 |
|------|----------|
| [《火焰图驱动性能循环 + 项目技能实施计划（2026-07-21）》](./superpowers/plans/2026-07-21-FLAMEGRAPH_PERF.md) | 在 macOS arm64 上建立可复现的火焰图工作流（samply/dtrace），采集 L1–L4 profile 并落地至少一项证据支撑的优化，固化为项目技能。 |
| [《客户端 acknodelay 修复 + FEC 启用 + ACK 批量优化（已完成）》](./superpowers/plans/2026-07-22-ACKNODELAY_FEC_OPTIMIZATION_DONE.md) | 修复客户端 UDP reader 的 `ack_nodelay` 参数传递、启用 FEC 参数建议与 ACK 批量发送；编译 / clippy / 单测 / e2e 全绿。 |
| [《热路径 is_closed() Mutex 回退修复（已完成）》](./superpowers/plans/2026-07-22-HOTPATH_IS_CLOSED_MUTEX_REGRESSION_DONE.md) | 消除 flush/recv 热路径上新增的 `is_closed()` Mutex 锁，把吞吐恢复到 README 记录水平。 |
| [《SMUX stream 逻辑泄漏修复计划（代理场景 RSS 上涨）》](./superpowers/plans/2026-07-22-SMUX_STREAM_LEAK_FIX.md) | 修复代理场景下 SMUX stream 无法从 map 移除、stream_count 无界增长导致 RSS 785MB→913MB+ 的泄漏（僵尸回收 + 惰性分配）。 |
| [《kcptun-server 性能优化实施计划（2026-08-01）》](./superpowers/plans/2026-08-01-SERVER_PERF_OPTIMIZATIONS.md) | 移除 server 每条会话/每包的固定开销：空闲唤醒、双重 DashMap 查找、`drain_new_streams` 全量克隆、阻塞式限速器等，不触碰 wire 格式。 |
| [《tcpraw RST 抑制 + kcp-rs TCP 接口实施计划（2026-08-01）》](./superpowers/plans/2026-08-01-TCPRAW_RST_KCP_RS_SURFACE.md) | 为 Linux tcpraw 传输加 RST 过滤与优雅关闭（含 iptables TTL-DROP 兜底），并在库层暴露 `connect_tcp` / `KcpTcpListener`（文档时代 API 名为 `KcpConn`）。 |
| [《项目布局优化迁移计划（2026-08-03）》](./superpowers/plans/2026-08-03-LAYOUT_REORGANIZATION.md) | 根目录精简到 8–10 个入口文件，脚本/文档/Docker 分类归入一级目录，保证 Makefile 与脚本引用同步、全部命令可运行。 |
| [《KcpListener ↔ TcpListener 接口对齐计划（2026-08-04）》](./superpowers/plans/2026-08-04-KCPCONN_LISTENER_TCP_ALIGNMENT.md) | 给 `KcpListener` 补齐 `accept_timeout` / `try_accept` / `take_error` 并让 `bind` 与 `tokio::net::TcpListener` 写法一致。 |
| [《KcpConn ↔ TcpStream 接口对齐计划（2026-08-04）》](./superpowers/plans/2026-08-04-KCPCONN_TCPSTREAM_ALIGNMENT.md) | 给 `KcpConn` 补齐 `shutdown` 半关闭、读写超时、`split`/`into_split`、`peek`、`take_error` 等 TCP 习惯方法。 |
| [《conn/listener 吞吐与 P99/P999 尾延迟综合优化计划（2026-08-05）》](./superpowers/plans/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY.md) | 尾延迟 Phase 0–4 已完成：空闲零 timer、内联发送、burst 批处理、flush 缓冲 reserve 等；余项按证据门控延期。 |
| [《KcpListener 多线程高并发优化（P0.2–P1.3，2026-08-05）》](./superpowers/plans/2026-08-05-KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md) | 落地资源有界 + 零分配批量收包 + 每 burst 一次锁 + 批量接收栈，并做 Go wire 兼容性交叉验证。 |
| [《P999 尾延迟细化：OwnedWriteHalf::write_all_shared（2026-08-12）》](./superpowers/plans/2026-08-12-p999-write-all-shared-refinement.md) | 消除 server 回声写路径的 notify→wake→drain→send 调度跳，把 Rust-tokio P999 从 ~1.8ms 继续压向 Go 的 ~0.8ms；A/B 已验证。 |
| [《入站 in-place CFB + null 少拷贝计划（已实现）》](./superpowers/plans/INBOUND_CFB_NULL_LESS_COPY.md) | 只针对入站解密/去头/喂 KCP 路径做 in-place CFB 与 null 少拷贝，先拿可复现 alloc 证据再改码，Phase 1–4 已落地。 |

## superpowers specs 设计规格与实施记录

| 文档 | 中文摘要 |
|------|----------|
| [《设计：火焰图驱动性能循环 + 项目技能（2026-07-21）》](./superpowers/specs/2026-07-21-FLAMEGRAPH_PERF_DESIGN.md) | 火焰图工作流设计稿：隧道 + 压测脚本、samply/dtrace 采集、热点映射到 PERF_OPTIMIZATION_PLAN 余项。 |
| [《全加密算法压缩火焰图分析报告（2026-07-22）》](./superpowers/specs/2026-07-22-ALL_CIPHER_COMP_FLAMEGRAPH_ANALYSIS.md) | 14 种 cipher × 压缩开关的火焰图对比分析（M1 机器），给出热点结论与待决策的优化建议。 |
| [《Spec：热路径 is_closed() Mutex → AtomicBool（实施记录）》](./superpowers/specs/2026-07-22-HOTPATH_IS_CLOSED_MUTEX_REGRESSION.md) | 该回归修复的改动文件清单与提交记录。 |
| [《Spec：SMUX stream 内存泄漏修复（实施记录）》](./superpowers/specs/2026-07-22-SMUX_STREAM_LEAK_FIX.md) | 泄漏修复的改动清单（stream.rs / session.rs 等）与验证记录。 |
| [《KcpConn / KcpSession 重构设计文档（2026-07-30）》](./superpowers/specs/2026-07-30-KCP_NETCONN_ABSTRACTION_ANALYSIS.md) | 把 kcp-rs 拆成通用 KcpConn（KCP 可靠传输）+ KcpSession（kcptun 加密会话层），API 对齐 tokio TCP 的分层设计。 |
| [《B2：crypto_buf → kcrypt-rs::wire 模块边界重组（已完成）》](./superpowers/specs/2026-07-31-MODULE_BOUNDARY_REORG.md) | 把 `crypto_buf` 迁移到 `kcrypt-rs::wire`，让 kcp-rs 完全去掉对 kcrypt-rs 的依赖；含依赖死循环的纠正记录。 |
| [《生产路径迁移计划（KcpConn/Smux 库栈 → kcptun 二进制）（2026-07-31）》](./superpowers/specs/2026-07-31-PRODUCTION_MIGRATION_PLAN.md) | M0 + M1-A 已实施：二进制改用库栈的迁移步骤与 §6 七项议题决议。 |
| [《smux-rs Builder 风格 API 简化设计（2026-07-31）》](./superpowers/specs/2026-07-31-SMUX_RS_SIMPLIFICATION.md) | 去掉 SmuxIo 中间层，Stream 直接实现 AsyncRead/AsyncWrite，`SmuxConn` 改用 Builder 构造以对齐 KcpConn。 |
| [《tcpraw RST 抑制 & kcp-rs TCP 接口设计（已实现，2026-08-01）》](./superpowers/specs/2026-08-01-TCPRAW_RST_KCP_RS_SURFACE.md) | 关闭 tcpraw 的两个缺口：入站 RST 过滤 + 库层 TCP 入口；已由 `feat-tcpraw-rst-kcp-rs` 分支 10 个提交实现。 |
| [《Spec：KcpListener ↔ TcpListener 对齐（实施记录，2026-08-04）》](./superpowers/specs/2026-08-04-KCPCONN_LISTENER_TCP_ALIGNMENT.md) | 监听器接口对齐的改动文件清单与验证记录。 |
| [《Spec：KcpConn ↔ TcpStream 对齐（实施记录，2026-08-04）》](./superpowers/specs/2026-08-04-KCPCONN_TCPSTREAM_ALIGNMENT.md) | 连接接口对齐的改动文件清单与验证记录。 |
| [《Spec：tokio server ACK 批量修复（实施记录，2026-08-04）》](./superpowers/specs/2026-08-04-TOKIO_ACK_BATCHING_FIX.md) | 修复 tokio server 的 ACK 批量发送问题（性能缺口，非正确性 bug）的改动记录。 |
| [《Spec：conn/listener 尾延迟优化（实施记录，2026-08-05）》](./superpowers/specs/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY_IMPLEMENTATION.md) | Phase 0–4 落地的 8 个提交与逐文件改动记录。 |
| [《Spec：KcpListener 并发优化（实施记录，2026-08-05）》](./superpowers/specs/2026-08-05-KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md) | P0.2–P1.3 + P2.1 部分落地的逐文件改动记录。 |
| [《多线程场景 P99 优化：input loop 内联发送策略（2026-08-05）》](./superpowers/specs/2026-08-05-P99_MULTITHREAD_INLINE_SEND_OPTIMIZATION.md) | 在 input loop 内联发送以消除跨任务唤醒，承接上一份 ACK batching 修复。 |
| [《Spec：advanced-API 延迟探针 + poll_read 合并（2026-08-06）》](./superpowers/specs/2026-08-06-ADVANCED_PROBE_READ_COALESCING.md) | 用 kcp-rs 高级 API 重写 P99/P999 探针并保持 RESULT 输出格式，同时实现 `poll_read_into` 合并读。 |
| [《Spec：恢复 Go 兼容的 fast retransmit（2026-08-06）》](./superpowers/specs/2026-08-06-FAST_RETRANSMIT_GO_COMPATIBLE.md) | 修复 KCP flush 中全部重传退化为 RTO 超时（200ms+）的问题，恢复 ~1 RTT 的快速重传，消除 3–4× 尾延迟劣化。 |

## 性能证据 evidence

> `evidence/` 为 `perf/opt-v2` 等性能改动的原始测量输出（O 编号对应任务书条目），供复现与审计。

| 文档 | 中文摘要 |
|------|----------|
| [《A0-1：测试门禁记录》](./evidence/A0-1-tests.txt) | `cargo test -p kcp-rs --features async` 全量测试输出（93 项等）。 |
| [《A0-2：fmt / clippy 门禁记录》](./evidence/A0-2-fmt-clippy.txt) | `cargo fmt --check` 与 workspace `clippy -D warnings` 的输出与退出码。 |
| [《A0-3：改动前基线 bench 日志》](./evidence/A0-3-bench-base.log) | 全量 bench 基线日志（smux_bench 吞吐/延迟等，Linux 4 核）。 |
| [《A0-3：改动后 bench 日志》](./evidence/A0-3-bench-post.log) | 与基线同 suite 的改动后复测日志，用于 A/B 对比。 |
| [《O0：CFB 微基准》](./evidence/O0-cfb-microbench.txt) | 各 cipher（aes / aes-128-gcm / null / 3des 等）加解密 ns/pkt 与 MB/s。 |
| [《O0：全量矩阵 bench 日志》](./evidence/O0-matrix-post.log) | 改动后 14 cipher × 压缩开关的 10 连接矩阵测试日志。 |
| [《O5：reed-solomon-simd 兼容性 spike 结论》](./evidence/O5-rs-simd-spike.txt) | 结论：该库用 GF(2^16) 与 16 位交织布局，与 KCP FEC（GF(2^8)）不可能字节一致 —— 关闭该项。 |
| [《O8：cpu_block 分配 A/B》](./evidence/O8-cpu-block-allocs.txt) | 池化后每次调用分配从 8.03 降到 4.03（字节 1200→168），含串行/并发延迟。 |
| [《O9：批量 CFB 解密微基准》](./evidence/O9-cfb-decrypt-bench.txt) | 批量 keystream 方案前后解密 ns/pkt 对比。 |
| [《O10-R：CRC 融合可行性微测》](./evidence/O10R-crc-fusion-micro.txt) | 逐字节融合比现有 SIMD CRC + memcpy 慢 24 倍，判定负收益关闭。 |
| [《O11：AeadCrypt::seal() 调用方审计》](./evidence/O11-seal-callers.txt) | 生产路径无 `seal()` 调用（走 `seal_into`），纯证据关闭、不改代码。 |
| [《O12：prepare_outbound 单次遍历 A/B》](./evidence/O12-prepare-bench.txt) | 1024 streams 交错 A/B：均值 -3.0%、中位 -6.1%。 |
| [《O13：直接读入 codec 缓冲 A/B》](./evidence/O13-direct-read-bench.txt) | 各配置前后吞吐对比（含小样本噪声说明）。 |
| [《O14：stream 查找缓存 A/B》](./evidence/O14-stream-cache-bench.txt) | 单条目缓存在 1/64 streams 下的命中与开销（约 -1% 噪声级）。 |
| [《O15：连接池 RSS 对比》](./evidence/O15-pool-rss.txt) | 移除 per-stream pool、改会话共享后，1024 流现场 RSS 下降 41.7%（-7.3 MB）。 |
| [《O16：--threads 延迟 A/B》](./evidence/O16-threads-latency.txt) | 共享 4 核机器上无分辨信号，结论附宿主负载说明。 |
| [《最终门禁：全 workspace 检查》](./evidence/final-gates-workspace.txt) | `cargo check/fmt/clippy/test --workspace` 收尾输出。 |

## 其他资源

| 文件 | 说明 |
|------|------|
| [logo.png](./logo.png) | 项目 logo 图片资源。 |

> 相关目录：Bug 报告与事故复盘统一放在仓库根目录 [`bugs/`](../bugs/)（不在本索引范围内）。

## 推荐阅读顺序

1. **初次了解项目**：`../README.zh.md` → [`PERF_OPTIMIZATION_PLAN.md`](./PERF_OPTIMIZATION_PLAN.md) → [`KCPTUN_RS_CODEBASE_AUDIT_REPORT.md`](./KCPTUN_RS_CODEBASE_AUDIT_REPORT.md)
2. **接手近期工作**：按时间读 5 篇 [handoff](#会话交接-handoff)，最新为 [`handoff-session-task-listener-2026-09-24.md`](./handoff-session-task-listener-2026-09-24.md)
3. **理解 listener 当前架构**：`handoff-session-task-listener-2026-09-24.md` → `perf-analysis-session-task-2026-09-24.md` → `perf-optimization-plan-session-task-listener.md` → `session-task-listener-close-lifecycle-review.md`
4. **复现性能结论**：先看 [`perf-v2-a0-change-breakdown.md`](./perf-v2-a0-change-breakdown.md)，再到 [evidence](#性能证据-evidence) 找对应编号的原始输出

---

## 已删除文档记录（2026-10-15 清理）

以下 11 篇文档因「方案已被取代 / 内容与现行实现不符」于 2026-10-15 从 `docs/` 删除（git 历史中仍可找回）：

| 已删除文档 | 删除原因 |
|------|----------|
| `docs/superpowers/plans/2026-08-12-goruntime-async-runtime.md` | goruntime 实现分支从未合入，项目现为 tokio-only |
| `docs/superpowers/plans/2026-08-02-P99_OPTIMIZATION_IMPLEMENTATION.md` | 被 2026-08-05 尾延迟计划明确取代 |
| `docs/superpowers/specs/2026-07-29-TCPRAW_TRANSPORT_DESIGN.md` | draft，被 2026-08-01 tcpraw spec 取代（TCP_REPAIR-only 假设作废） |
| `docs/OPT_CPU_BLOCK_SNAPPY_INVESTIGATION.md` | smol 后端已删除、前提消失；offload 阈值已按其他方案落地 |
| `docs/kcp-rs-listener-simplify-plan.md` | 方案已实施（session-as-tasks 上线），文档仍标「未动代码」、现状描述过时 |
| `docs/KCP_RS_GO_CONCURRENCY_STABILITY_TEST_PLAN.md` | 依赖「tokio 与 smol 双后端一致性」前提，smol 已删除 |
| `docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md` | 面向 worker 线程模型，已被 session-as-tasks 架构取代 |
| `docs/superpowers/plans/2026-08-11-KIO_RS_OPTIMIZATION_PLAN.md` | 对象 `kio-rs` 已更名 `knet-rs`，余项引用不存在的文件 |
| `docs/superpowers/plans/2026-08-11-tokio-tail-latency-diagnosis.md` | 状态「待决策」，P999 议题已由 08-12 优化闭环 |
| `docs/superpowers/plans/2026-08-04-KCP_RS_TEST_STRATEGY.md` | draft，被 2026-08-05 测试方案取代 |
| `docs/SMOKE_TEST_COVERAGE_ANALYSIS.md` | 分析对象为 7 月旧脚本（smol / kio-rs 时代），与当前脚本不符 |
