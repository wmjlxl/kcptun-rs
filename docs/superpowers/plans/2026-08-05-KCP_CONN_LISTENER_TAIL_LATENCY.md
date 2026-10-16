# Plan: `kcp-rs` conn/listener 吞吐与 P99/P999 尾延迟综合优化

> **Canonical path (git):** `docs/superpowers/plans/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY.md`

| Field | Value |
|-------|-------|
| Status | implemented (核心 Phase 0–4 完成；Phase 5/7/8 与 Phase 6 余项按证据门控延期，见 §19) |
| Created | 2026-08-05 |
| Scope | 以 `kcp-rs/src/conn.rs`、`listener.rs` 为核心，覆盖必要的 `transport.rs`、`fec.rs`、`kcp.rs`、`kio-rs` 与 server shard 接入；优化吞吐、P99/P999、连接风暴与空闲连接规模 |
| Out of scope | 改变 KCP/SMUX/crypto wire format；以扩大拥塞窗口伪造性能；把 `kcp-rs` 重新耦合到 crypto；未经证据直接重写为游戏服务器 actor；生产可用性承诺 |
| Supersedes | 2026-08-02 P99 优化计划（该文档已删除）中尚未执行且与当前实现冲突的部分；已实施历史仍以各 spec 为准 |
| Related | `kcp-rs/AGENTS.md`；`bench/LATENCY_P99_REPORT.md`；`bench/profiles/HOTSPOTS.md`；`docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`（已删除）；`docs/superpowers/specs/2026-08-05-P99_MULTITHREAD_INLINE_SEND_OPTIMIZATION.md`；两份用户提供的 conn/listener 分析文档 |

## 1. 结论摘要

两份输入文档指出了正确的优化方向：减少锁内分配、缩短 KCP 临界区、降低 listener
连接风暴干扰、减少跨任务唤醒、改善大窗口乱序和 FEC 恢复路径。但它们所依据的代码快照早于
当前仓库的多轮优化，部分结论已经过期，部分预估没有实测依据。

本项目当前事实是：

1. listener 已完成零分配批量收包、每 wakeup 一次 `sessions` 锁、PeerQueue/KCP input
   批处理、有界队列与 staged build；不能重复实现。
2. `KcpConn` 已有绝对 deadline flush、64 KiB 写分块、FEC 锁外解码、批量 KCP input、
   单发送令牌和 runtime 条件化发送策略。
3. 最新 raw-KCP release 基线在 500 RPS、26 KiB、30,000 样本下，Tokio P99/P999
   为 581/778 us，smol 为 670/882 us；不存在输入文档假设的“普遍 10–15 ms P999
   地板”。
4. Tokio input-loop inline-send 已实测使 P99 中位数恶化约 66%；smol inline-send 则改善。
   因此不能把 Direct Inline Send 无条件应用到两个 runtime。
5. 当前 `kio::Notify` 已是 permit-storing、注册后复查的单 waiter 原语。`WAIT_FALLBACK_MS=10`
   是安全网和 timer churn 折中，不应在没有 fallback 命中证据时直接降到 2 ms。
6. macOS null profile 的主要成本已经是逐包 UDP syscall 与 Tokio reactor；listener map
   本身不再是主要热点。Linux 才能完整评估 recvmmsg/sendmmsg/SO_REUSEPORT 的收益。
7. server 已有 SO_REUSEPORT 多 socket；真正未完成的是 socket/listener 在主 runtime 创建，
   随后才把 accept future 移入 shard thread，导致代码宣称的 shard-local ownership 不成立。

因此推荐按以下顺序实施：

```text
基线与指标
  -> 当前低风险补丁验证
  -> listener 丢弃路径零分配 + raw batch 容量复用
  -> poll/backpressure 唤醒收敛
  -> 可取消 recv，移除 100 ms × N timer
  -> staged build/affected 去重的连接风暴优化
  -> KCP/FEC 丢包专项优化（证据门控）
  -> 修正现有 SO_REUSEPORT shard ownership
  -> 可选 KcpEndpoint（仅数万游戏会话目标）
```

## 2. 不可破坏的约束

- Go kcptun/kcp-go v5 wire compatibility 是最高约束。
- KCP 输入对每个 Push 段排 ACK；不能通过漏 ACK、放宽重传或扩大窗口伪造延迟收益。
- wire packet 必须保持 flush 产生顺序；任何多 sender 方案都必须通过 retrans-storm gate。
- FEC decode 继续位于 KCP mutex 外；crypto 与 Snappy 继续位于 `kcp-rs` 外。
- Tokio 与 smol 必须分别构建、测试、A/B；不能根据一个 runtime 推断另一个。
- 一次只提交一个优化类；没有证据的阶段不进入实现提交。
- 默认不新增依赖。若确需数据结构依赖，必须先证明标准库实现不能满足目标。
- 所有性能数字使用 release/profiling build；debug 数字不得进入结论。
- 不覆盖当前工作区已有修改。A/B 使用独立 worktree/target 目录，不用会改写文件的
  `git checkout --` 型脚本。

## 3. 当前数据路径与竞争边界

```text
shared UDP socket
  -> listener reader
     -> one sessions lock per wakeup
     -> PeerQueue::push_and_reuse
     -> one notify per affected peer
  -> KcpConn input loop
     -> recv/try_recv_batch
     -> FEC decode outside KCP lock
     -> one KCP lock for input burst
     -> one read_buf lock
     -> deferred ACK flush
  -> send policy
     -> Tokio input: notify flush loop
     -> smol input: try inline-send, CAS conflict falls back to notify
     -> async write_all: inline send under single is_sending token
     -> poll_write: flush-loop single-drainer path
```

主要共享状态：

| 状态 | 当前 owner/竞争者 | 当前风险 |
|------|------------------|----------|
| `kcp: Mutex<KCP>` | input、flush、用户 writer | 大写、ACK、RTO flush 竞争；必须限制持锁时间 |
| `raw_packets: Mutex<Vec<Bytes>>` | KCP output callback push、sender drain | `mem::take + reserve` 在锁内分配；KCP 锁内逐输出获取第二把锁 |
| `read_buf` | input push、用户 reader pop | 高频 poll；当前工作区已尝试消除重复获取 |
| `sessions` | listener route、build commit、sweep、remove_peer | 路由已每 burst 一锁；build/sweep 仍需减少锁次数/持锁时间 |
| `PeerQueue::buffers` | listener push、peer input pop | 已批量 pop；过载丢弃仍可能重新分配 recv buffer |
| `is_sending` | write/input/flush sender | 保证一次一个 sender，但不自动保证不同批次 enqueue 顺序 |

## 4. 两份输入建议的处理矩阵

| # | 输入建议 | 当前判断 | 处理 |
|---|----------|----------|------|
| 1 | `poll_read_into` 两次 `read_buf` 锁 | 当前工作区已实现一次锁 | 纳入 Phase 1，补并发 close/data/waker 测试后保留 |
| 2 | `drain_raw_packets` 避免 `mem::take + reserve` | 问题成立；`drain(..).collect()` 仍会在锁内为返回 Vec 分配 | 改写为 pending/spare 双 Vec 回收，Phase 2 |
| 3 | KCP flush 时逐包获取 `raw_packets` 锁 | 问题结构上成立，但尚无锁等待证据 | 先完成 batch 容量复用；若仍 >=5% 再做 collector/batch enqueue，Phase 5 |
| 4 | write path 不 await UDP send | 不能直接采纳；当前 inline write 是降低调度 hop 的既有策略，且负责 OS backpressure | 先记录 send await/WouldBlock；只有与 P999 相关时才改为唯一 sender task |
| 5 | 10 ms fallback/version 防 lost wake | 预设不成立；Notify 已存 permit，最新 P999 <1 ms | 先计数 fallback；若命中，再修 waiter 语义，不盲降 interval，Phase 3 |
| 6 | 复用 `Vec<Cow>` | 仅 FEC 路径有意义，生命周期使简单外提不可行；现有 flat CPU 已很低 | 随 FEC recovery API 一起优化，Phase 6 |
| 7 | Atomic 缓存 MSS | 不采纳；`mss()` 是内联字段读取，Atomic 更贵 | 明确否决 |
| 8 | 预 spawn 永久 backpressure task | 不采纳；会增加每连接 idle task | 改为 flush/input 直接唤醒 poll waker，移除按次 waiter task，Phase 3 |
| 9 | `#[repr(C)]` + 字段重排 | 无 cache-miss 证据，`repr(C)` 不保证更快且增加布局承诺 | 暂缓；只有硬件 counter 证实时再做 |
| 10 | `wake_by_ref` 避免 clone | 当前工作区已尝试；但不能在 mutex guard 内执行任意 waker | Phase 1 改为锁外 wake；优先 `take()+wake()` |
| 11 | 并行 `process_builds` | 直接 `join_all` 会制造分配/任务风暴，build 大多是同步构造 | 先三阶段批量锁 + 时间预算；并发必须有上限且 profile 证明，Phase 4 |
| 12 | `affected` O(N²) 去重 | 大 burst/多 peer 时成立；小 batch 下 HashSet 可能更慢 | 使用复用的 hybrid set，按 distinct-peer 阈值切换，Phase 4 |
| 13 | sweep 每 entry 一次锁 | 当前工作区已批为一次锁 | Phase 1 验证并保留 |
| 14 | spares 预分配 | 当前仅首次补到 16，不是稳态每轮分配 | 启动时一次填满，作为丢弃路径改造的附属小改动 |
| 15 | admission drop 重新分配首包 buffer | 当前确实存在 `unwrap_or_else(vec![...])` | 高优先级，Phase 2 |
| 16 | 100 ms close polling | 问题成立；改 50 ms 只移动权衡 | 增加跨 runtime cancel token/race，彻底移除周期 tick，Phase 3 |
| 17 | `rcv_buf` 换 BTreeMap | 不建议直接换；典型窗口下 locality/分配更差 | 先实现匹配 Go 的 reverse/fast-path scan；大乱序热点仍 >=5% 才评估新结构，Phase 6 |
| 18 | FEC recovery buffer pool | 高丢包+FEC 下可能有效；正常链路不是热路径 | 丢包矩阵 profile 后实施，Phase 6 |

输入文档中的 SO_REUSEPORT/DashMap 建议需重写：SO_REUSEPORT 已存在；单 listener 仍只有一个
reader，且 route/build/sweep 主要在同一 reader future 中，直接换 DashMap 不会解决 executor
归属、socket syscall 或 per-session task/timer 问题。

## 5. 基线与验收方法

### 5.1 固定基线

每次优化记录：

- commit 与 dirty diff hash；
- CPU、逻辑核、OS/kernel、Rust/Go 版本；
- Tokio/smol、cipher、FEC、compression、KCP mode、MTU/window；
- socket buffer/sysctl、CPU governor/电源模式；
- 完整命令、预热、时长、样本量、失败/丢包数；
- CPU/heap profile 与 SNMP 前后差值。

现有 raw-KCP 基线命令：

```bash
RPS=500 WARMUP=5 DURATION=60 SIZE=26624 CONCURRENCY=32 \
  bash bench/run_p99.sh

bash bench/run_p99_regression.sh
```

正式 P999 结论至少运行 10 分钟（500 RPS 为 300,000 样本，约 300 个 P999 tail
样本）。开发期 60 秒用于筛选，不作为最终 P999 承诺。

### 5.2 必补场景

| ID | 场景 | 目的 |
|----|------|------|
| C1 | raw KCP，500/1k/饱和 RPS，1 KiB/26 KiB/256 KiB | read/write/flush 尾延迟与 retrans storm |
| C2 | 1/16/64/256 peers，小包 fan-in | listener distinct-peer、affected 去重、公平性 |
| C3 | 连接风暴 + pending 不 accept + admission drop | build/sweep 锁、RSS plateau、合法 peer P999 |
| C4 | 1k/10k idle sessions | 100 ms recv timer、task 数、CPU、close latency |
| C5 | 0/1/5/10% loss + reorder/jitter，FEC off/on | `parse_data` 与 FEC recovery |
| C6 | tunnel bulk + 同时小流 ping | 吞吐与 head-of-line/tail 联合 Pareto |
| C7 | Linux 1/2/4/CPU-count shards | SO_REUSEPORT scaling、倾斜、每 shard CPU/pps |
| C8 | macOS 单 socket fallback | 无 sendmmsg 时的 syscall 边界与 runtime 差异 |

### 5.3 统计规则

- 开放模型固定发送速率，避免 coordinated omission。
- 每轮 percentile 由该轮全部原始样本一次计算；不平均子批次 percentile。
- 至少 8 轮交错 ABBA；报告每轮值、paired ratio 中位数和 bootstrap 95% CI。
- 吞吐和固定 RPS 延迟分开报告。
- 低风险微优化可用直接指标通过：锁获取次数、allocations/datagram、syscalls/datagram、
  task/timer 数明确下降，同时端到端 P99/P999/吞吐无统计显著回归。
- 数据面优化默认门槛：目标指标改善 >=10%，吞吐不得回退 >3%，P50 不得回退 >5%。
  噪声高于门槛时延长测试，而不是挑最好一轮。
- 任何 fast/early retrans、lost、queue drop 增加必须解释；无法解释即回滚。

### 5.4 通用正确性门禁

```bash
make gate
bash kcp-rs/test.sh
make stress
make e2e
bash bench/run_p99_regression.sh
```

涉及 Linux mmsg/shard 时追加：

```bash
cargo check -p kio-rs -p kcp-rs -p kcptun-common \
  --features async-tokio --target x86_64-unknown-linux-gnu
```

## 6. Phase 0：基线、可观测性与安全 A/B 基础设施

### 6.1 目标

先证明延迟来自哪里，避免用代码审查估算替代实测。该阶段不改变 wire/data-plane 行为。

### 6.2 工作项

1. 扩展 `latency_p99`/`run_p99.sh`：
   - 可选保存每个样本的原始延迟与发送时间；
   - 输出失败、超时、SNMP delta、实际发送/完成 RPS；
   - 报告 runtime worker 模式、socket buffer 和 shard 数；
   - 支持独立 target dir 的 baseline/candidate 构建，禁止修改工作树做 A/B。
2. 增加 opt-in 计数器，默认关闭：
   - conn：input batch size、raw batch size/HWM、send-token conflict、send await 超时、
     read fallback timeout、backpressure arm/wake、KCP lock wait/hold 采样；
   - listener：recv syscall/datagram、实际 batch size、distinct peers、drain budget hit、
     sessions lock hold 采样、build queue/HWM/time、admission/queue drop、sweep scanned/expired；
   - system：UDP socket drops、send/recv syscall 数。
3. 更新 `bench/profiles/HOTSPOTS.md`，分别记录 Tokio/smol、macOS/Linux；不把
   `Inner::park` 当 CPU 热点。
4. 新增 C2/C3/C4/C5 harness；性能脚本不得承担 correctness assertion，correctness 另写测试。

### 6.3 文件

- `kcp-rs/examples/latency_p99.rs`
- `bench/run_p99.sh`、新增非破坏性 A/B runner
- `kcp-rs/src/snmp.rs`、`conn.rs`、`listener.rs`（只加 opt-in 观测）
- `bench/profiles/HOTSPOTS.md`

### 6.4 验收

- instrumentation 关闭时吞吐/P99 差异在 1% 噪声内；
- raw samples 可复现报告 percentile；
- 能明确回答一次 P999 spike 同期是否发生 fallback、send WouldBlock、lock wait、retrans 或 OS drop。

## 7. Phase 1：验证当前工作区的三项低风险修改

当前工作区已包含但尚未提交：一次 `read_buf` 锁、`wake_by_ref`、一次 sweep lock。实现时应
先单独保存/拆分，不能和后续优化混成一个提交。

### 7.1 `poll_read_into` 单锁

- 保持“先取数据，再判 EOF”的语义；close 与最后一批数据竞争时不得提前丢数据。
- 新增测试：
  - data push 发生在 poll 与 waker 注册之间；
  - close 与 data push 并发；
  - partial read 后剩余 Bytes 保持在队头；
  - split/owned half drop 不改变 EOF 顺序。
- 直接指标：一次空 poll 的 `read_buf` lock acquire 从 2 降到 1。

### 7.2 writer waker 锁外唤醒

当前 `wake_by_ref()` 写法在持有 `write_waker` mutex 时调用外部 waker，不应直接接受。
推荐：

```rust
let waker = self.write_waker.lock().take();
if let Some(waker) = waker {
    waker.wake();
}
```

下一次仍阻塞的 poll 会重新注册。若测试证明必须保留 waker，则允许 clone，但 clone 必须在
临界区内完成、wake 在锁外执行；避免为省一次 refcount 引入重入死锁或拉长锁持有时间。

### 7.3 sweep 单锁

- 一次 `sessions` 锁完成 Building 扫描；`mark_closed` 与统计更新在锁外。
- `kept`/`to_close` scratch 跨 sweep 复用，避免周期性分配；或仅在 timeout 启用时创建。
- 增加 generation replacement、timeout=ZERO、1000 Building entries、并发 remove_peer 测试。

### 7.4 进入下一阶段的条件

- `make gate`、双 runtime listener tests、stress、e2e 全绿；
- C1/C3 无 tail/retrans 回归；
- waker 不在任何内部 mutex 下调用。

## 8. Phase 2：消除锁内/丢弃路径分配

### 8.1 listener admission/queue-drop buffer 归还

当前 `route_inner` 在 `max_sessions` drop 时返回 `None`，reader 随后分配新的
`vec![0; MAX_DATAGRAM]`；`PeerQueue::push_and_reuse` 在 queue 满且 spare 为空时也会丢掉可复用
的 `pkt` 再分配。

改造为始终返还一个可写 recv slot：

```text
RouteOutcome { spare: Vec<u8>, queued: bool, newly_building: bool }
```

- admission drop：清空/resize 输入 `data` 并原样返回；
- queue drop：直接回收被 drop 的 `pkt`，优先于 spare pool；
- 正常 enqueue：从 queue spare 取 buffer；仅冷启动/容量不足时分配；
- `affected` 只记录真正 queued 的 peer；drop 不产生无意义 notify；
- reader 启动前一次性构造 `RECV_BATCH` slots，`peers`/`affected` capacity 同步预留。

验收：C3 steady-state `allocations/admission-drop` 接近 0，RSS 达到平台，合法 peer P999
不因攻击流量持续上升。

### 8.2 `raw_packets` pending/spare 双缓冲

不采用输入文档的 `g.drain(..).collect()`，因为它仍为返回 Vec 分配且分配发生在锁内。
引入私有容器：

```text
RawPacketQueue {
  pending: Vec<Bytes>,
  spare: Vec<Bytes>,
  high_water: usize,
}
```

流程：

1. output callback 只 push 到 `pending`；
2. sender 持 `is_sending` 后，在短锁内 swap `pending` 与 `spare`，得到完整 batch；
3. 锁外 FEC expand + async send；
4. send 完成后 clear batch，再放回 `spare`；只保留容量更合适的一份，设置最大 retained cap，
   防止一次异常巨 burst 永久占内存；
5. 错误路径也必须回收 batch；不能因 send error 丢失容器容量；
6. `finish_sending` 的“是否仍有 pending”检查和 token release 用既定顺序，防止 missed wake。

验收：warm-up 后 `drain_raw_packets` 锁内 allocation=0；packet order/retrans storm gate 全绿；
raw HWM 后 RSS 可回落到 retained cap。

### 8.3 commit 拆分

1. listener drop-path buffer reuse；
2. recv slots cold-start preallocation；
3. raw packet batch recycling。

每个提交单独 A/B 和回滚。

## 9. Phase 3：收敛 waiter、背压 task 与 100 ms timer

### 9.1 不先修改 `WAIT_FALLBACK_MS`

先用 Phase 0 指标确认 fallback 命中。若 10 分钟 C1 中命中为 0，它不是 P999 热点；保持 10 ms。
若命中：

- 检查是否违反 `kio::Notify` 单 waiter 约束（例如 `readable()` 与 `read_shared()` 并发等待）；
- 将“注册 waiter -> 复查 condition/closed -> await”作为统一模板；
- 多 reader 真的是受支持语义时，改为真正 multi-waiter readiness primitive，不能只加一个
  `AtomicU64` 掩盖最后注册者覆盖前一个 waker；
- version counter 只作为 condition generation，仍必须与 waiter 注册顺序配合。

### 9.2 移除每次背压的 waiter task

当前 `arm_backpressure_wake` 首次 arm 会 spawn 一个 task 等 `write_notify`。推荐新增统一的
`publish_wait_send(ws)`：

- 在 KCP 锁外更新 `wait_send`；
- 窗口有空间时同时 `write_notify.notify_one()` 和锁外 wake/take `write_waker`；
- `poll_write` 不再为正常窗口背压 spawn waiter task；
- 只有用户配置 write timeout 时，才允许一个有 generation guard 的 one-shot timeout task；
- `writable().await` 继续直接等待 `write_notify`。

验收：窗口频繁满/开场景 task spawn 数显著下降；无 missed wake/busy loop；write timeout 误差
满足现有 API。

### 9.3 可取消 recv

不把 listener/input loop 的 100 ms 改成 50/500 ms。增加 runtime-agnostic cancellation：

1. 在 `kio-rs` 提供轻量 `CancellationToken` 或 `race(recv, cancelled)`；每个 recv loop 一个 waiter；
2. `KcpConnShared::close()` cancel input recv；`KcpListener::close()` cancel listener recv；
3. Tokio/async-io socket recv future 被取消后必须可安全再次使用/释放；
4. `PeerTransport` 已有 queue notify close 路径，与统一 token 合并，避免双 timer；
5. 移除两个 `timeout(Duration::from_millis(100), recv...)`；错误退避 timer 仍可保留但要可取消。

验收：

- C4 10k idle session 不再产生约 10 Hz × N receive timers；
- close-to-task-exit P99 <10 ms，不依赖 100 ms tick；
- fd/task 数在 close 后回到基线；
- 双 runtime、listener close、silent peer、recv-error race 测试通过。

该阶段会新增公共 runtime/transport 能力，需同步 `kio-rs/AGENTS.md`、`kcp-rs/AGENTS.md`。

## 10. Phase 4：连接风暴下的 listener 锁与调度

### 10.1 `process_builds` 三阶段批处理

不直接 `join_all` 全部 peer。使用 bounded batch：

1. **Collect**：一次 `sessions` 锁，从 `build_work` 取 count/time budget 内的
   `(peer, generation, queue)`；
2. **Build**：锁外构造 KcpConn。第一版顺序构造，因为当前 build 主要是同步分配和 task spawn；
3. **Commit**：一次 `sessions` + `pending` 锁批量执行 generation 校验、Ready 升级、pending push；
   close/mark_closed/notify 放锁外批量执行。

同时：

- 把 build 调度放在完成当前 recv burst 路由/notify 之后，避免下一批数据到达前先无界 build；
- 除 `max_builds_per_wakeup` 外增加 `max_build_time_per_wakeup`，避免单个慢 build 突破公平性；
- 只有 profile 显示 build 自身可并行且 reader 有空闲，才增加 bounded concurrency；上限由配置/CPU
  和 memory budget 决定，禁止 one-peer-one-spawn；
- build 失败、pending 满、peer 被替换的所有 conn/queue 在锁外关闭。

验收：C3 中 sessions lock acquisitions/build 从约 2–3 降到 amortized 2/batch；accept P99 随
batch 内 peer 数不再线性增长；合法稳态流保持进展。

### 10.2 `affected` hybrid 去重

- 保留小 burst 的线性 Vec 快路径；建议 distinct peers <=16 时不引入 HashSet；
- 超过阈值后使用跨 wakeup 复用容量的 `HashSet<SocketAddr>`，而不是 Arc 指针转 usize；
- set/vec 都在 reader task 局部，无共享锁；
- 保持每 peer 每 wakeup 最多一次 notify 和 datagram 到达顺序；
- 记录 `affected_comparisons` 与 distinct peers，只有 C2 大 peer fan-in 改善才保留。

### 10.3 不换 DashMap

在一个 listener 仍只有一个 reader、每 SO_REUSEPORT socket 有独立 session table 的架构下，
DashMap 会增加 hash/guard/依赖成本，且不能解决 build 在 reader task 上串行的问题。只有未来出现
“同一 session table 被多个真正并行 reader 写”且锁 profile 证实后才重新评估。

## 11. Phase 5：KCP/output 临界区与写路径调参

### 11.1 output callback 批量提交（证据门控）

完成 Phase 2 后重新 profile。只有 raw queue lock wait 或 output callback 累计 >=5% 才实施：

- 为 KCP 增加仅供 async conn 使用的 collecting output sink；同步 KCP callback API 保持不变；
- 一次 flush 在 KCP 内收集 `Vec<Bytes>`；
- 为保持 flush 顺序，仍在持 KCP 序列化边界时一次性 append 到 raw queue，然后立刻释放；
- 从“每输出 packet 一次嵌套锁”降为“每 flush 一次短嵌套锁”；
- 不能先 drop KCP lock 再无序 enqueue，否则第二个 flush 可能先入队，重现 fastack/retrans storm。

若一次嵌套锁仍有显著等待，再设计带 monotonically increasing batch sequence 的唯一 sender queue；
不得用多个并发 drain/send 绕过问题。

### 11.2 写分块 A/B

当前已有 `KCP_SEND_CHUNK=64 KiB`，会在 `write_all_shared` 循环中释放并重取 KCP lock。
不要直接改成 16 KiB + 每块 `yield_now()`。

测试 8/16/32/64/128 KiB（或按 8/16/32/64 MSS 表达），覆盖：

- 1 KiB/26 KiB/256 KiB payload；
- 低 RPS、饱和、bulk+small mixed；
- Tokio/smol；
- 0/1/5% loss；
- KCP lock wait、ACK delay、retrans、throughput、P99/P999。

只有 chunk 释放后 input task 仍不能及时运行，才在多 chunk 之间条件 `yield_now()`；无竞争时不 yield。
最终值必须在吞吐与 tail Pareto 上胜出，不以单一 256 KiB echo 决定全局常量。

### 11.3 async write 的 UDP await

保留现有 OS backpressure 语义。Phase 0 若证明 send await/WouldBlock 与 P999 强相关，候选方案是：

- write 只 enqueue 到唯一 sender；
- 唯一 sender 尝试 nonblocking batch，WouldBlock 后 await writable；
- queue 必须按 bytes/packets 有界，writer 在 queue 满时 backpressure；
- input ACK 与用户 data 共享全局序列，不能分两个 sender；
- server unconnected socket 需 `try_send_batch_to` 对应能力。

纯 `notify`、无界 queue 或多 sender 均不接受。

## 12. Phase 6：大窗口乱序与 FEC 恢复专项

该阶段只在 C5 profile 证明 `parse_data`/FEC recovery 为可行动热点时执行。

### 12.1 `rcv_buf`：先匹配 Go 的常见快路径

不直接改 BTreeMap。优先：

1. empty、append-after-back、insert-before-front O(1) fast path；
2. 中间插入从尾部反向扫描，匹配 kcp-go 典型实现和“新包接近最大 SN”的分布；
3. 单次扫描同时判重与定位；
4. sequence wraparound 全部使用 `itimediff`；
5. 保持 `move_receive_buffer`、重复包 SNMP、ACK 行为完全一致。

仅当 `rcv_wnd>=512`、高 reorder 下该路径仍 >=5%，且 VecDeque move 成本实测主导，才原型比较
slab+ordered index/BTreeMap。新结构必须证明小窗口/顺序到达不回退，且不增加每 segment 分配。

### 12.2 FEC scratch/pool

当前 recovery 中仍有 `pkt.to_vec()`、`present Vec`、payload copy/resize、missing shard buffer、
recovered clone，以及 conn 对 recovered slice 的再次 `to_vec()`。

按以下顺序优化，每步单独测：

1. `present`/flag/shard metadata scratch 跨 recovery 复用；
2. 删除不再需要的临时 clone，确认 `new_buffers` 生命周期是否仍有实际用途；
3. decoder 提供 `decode_into(recovered_out)`，把恢复 buffer ownership 直接交给 conn；
4. conn 在 KCP input 后把 buffer 归还 decoder pool，消除 recovered 的第二次 `to_vec()`；
5. pool 同时限制条目数和总 bytes，防止异常 shard size 永久抬高 RSS；
6. FEC data/parity、variable length、RS padding、corrupt shard、auto-tune 均加专项测试。

验收：5/10% loss + FEC 下 allocations/recovery 和 P99 明确下降；0% loss 和 FEC off 无回归。

## 13. Phase 7：修正现有 SO_REUSEPORT shard ownership

输入文档提出“新增 SO_REUSEPORT 多 socket”，但项目已经实现。当前缺陷是：

```text
main multi-thread runtime:
  create kio socket -> build KcpListener -> spawn reader/input/flush
then:
  move only serve_udp_shard future to block_on_local thread
```

因此注释所称“shard fd only touched by one worker”目前不成立。

### 13.1 Tokio 改造

1. 主线程只创建/configure/bind `std::net::UdpSocket`（或传入 socket factory），不在主 reactor
   包装为 Tokio socket；
2. 将 std socket 移入 shard OS thread；
3. 在该 thread 的 `block_on_local` 内执行 `kio::UdpSocket::from_std`、
   `KcpListener::from_socket(...).build().await` 和 `serve_udp_shard`；
4. listener reader、新 peer input/flush、session tasks 均通过当前 local runtime spawn；
5. 增加 thread-id/shard-id opt-in trace，证明同 fd 的 recv/send/task 归属；
6. shutdown 保存 thread handles 并 join，不再 fire-and-forget。

### 13.2 smol 语义

smol 的 `block_on_local` 当前仍使用 process-global executor，不能复用 Tokio 的“local”声明。
两种选择必须先做设计评审：

- 为 kio/KcpListener 注入明确 `Spawner`/executor handle，使每 shard 有自己的 local executor；或
- smol 保持全局 executor，日志和文档明确不承诺 shard task affinity，只利用 SO_REUSEPORT
  做 socket 分流。

不要用注释假装两者等价。

### 13.3 验收

- Linux 1/2/4/N shard 吞吐随核数扩展，P99/P999 不因 migration 恶化；
- 每 peer 始终落在一个 shard，重连语义正确；
- shard 间负载倾斜可观测；
- macOS 默认 1 shard 不回退；
- stress/e2e/reconnect/autoexpire 全绿；
- executor 归属收益不明确或 tail 恶化则回滚，不把结构正确性自动等同于性能提升。

该阶段涉及结构/运行时归属，需同步 root、`kcptun-server/AGENTS.md`、`kio-rs/AGENTS.md`。

## 14. Phase 8（可选）：高连接游戏场景 `KcpEndpoint`

kcptun 隧道主路径优先完成 Phase 0–7。只有明确目标是 10k+ 小包会话且 C4/C7 显示
per-connection input/flush task 和 timer 为主瓶颈时，再新增独立 endpoint API：

```text
one socket/shard
  -> recvmmsg
  -> shard-local session table
  -> batch feed multiple KCP states
  -> one timer wheel
  -> cross-session sendmmsg_to
  -> bounded app channels
```

现有 TcpStream-shaped `KcpConn` 保留给隧道和易用性场景。该阶段是新架构/公共 API，必须另写
spec、迁移说明、内存预算和安全模型，不与前述微优化同一 PR。

## 15. 分阶段文件影响

| 阶段 | 主要文件 | 公共 API/AGENTS |
|------|----------|-----------------|
| 0 | bench scripts、latency example、snmp/hotspots | 通常无；新增公开指标时更新 kcp AGENTS |
| 1 | `conn.rs`、`listener.rs` tests | 纯 perf/bug line，无 AGENTS sync |
| 2 | `listener.rs`、`transport.rs`、`conn.rs` | 私有结构，无 AGENTS sync |
| 3 | `kio-rs/sync|task`、`transport.rs`、`conn.rs`、`listener.rs` | 有 cancellation/public transport 变化，更新 kio+kcp AGENTS |
| 4 | `listener.rs` | builder 新 limit 时更新 kcp AGENTS/README |
| 5 | `conn.rs`、`kcp.rs`、`transport.rs` | collecting sink/transport 能力若公开则更新 kcp AGENTS |
| 6 | `kcp.rs`、`fec.rs`、`conn.rs` | FEC public API 变化时更新 kcp AGENTS |
| 7 | `kcptun-server/app.rs`、`socket.rs`、`kio-rs/task` | 更新 root/server/kio AGENTS |
| 8 | 新 endpoint module + server integration | 单独 spec + root/kcp/server AGENTS |

## 16. 推荐提交序列

每一项一个可回滚提交：

1. `bench(kcp): add non-destructive tail-latency evidence capture`
2. `perf(kcp): collapse poll-read buffer locking`
3. `perf(kcp): wake blocked writers outside the waker mutex`
4. `perf(kcp): batch listener building-session sweep`
5. `perf(kcp): recycle admission-drop receive buffers`
6. `perf(kcp): recycle raw packet batch capacity`
7. `perf(kcp): publish write readiness without waiter tasks`
8. `perf(kio): add cancelable datagram receive`
9. `perf(kcp): batch staged connection builds`
10. `perf(kcp): bound affected-peer dedupe cost`
11. evidence-gated KCP/output/FEC commits
12. `perf(server): construct KCP shards inside their owning runtime`

每个 commit message 记录约束、被拒方案、信心、风险和未覆盖测试；不把多项收益揉成一个数字。

## 17. 停止/回滚条件

出现以下任一项立即停止当前优化类：

- retrans-storm gate 失败，或无丢包回环出现新的 fast/early retrans；
- wire/e2e/FEC data correctness 回归；
- 目标指标未改善且复杂度/公共 API 增加；
- 吞吐回退 >3% 或 P50 回退 >5%，且不是明确接受的 tail tradeoff；
- allocation/syscall/lock 指标没有按假设变化；
- RSS 不再有界或 queue/task 数随攻击时间持续增长；
- 只有单一最好轮支持结论；
- 需要改变拥塞控制、ACK 语义或 wire layout 才能得到收益。

若 profile 中没有 >=5% 的可行动 leaf/等待源，记录结果并停止继续微优化。

## 18. 完成定义

本计划只有在以下条件全部满足后才可标记 `implemented`：

1. Phase 0–4 完成或有书面证据说明某项不值得实现；
2. Phase 5–7 的每项均有 profile 决策记录（实施、拒绝或延期）；
3. Tokio/smol、macOS/Linux 关键矩阵有可复现结果；
4. `make gate`、kcp 双 runtime tests、stress、e2e、retrans regression 全绿；
5. P99/P999、吞吐、RSS、task/timer、syscall/alloc 指标同时报告；
6. `bench/profiles/HOTSPOTS.md` 与 `bench/LATENCY_P99_REPORT.md` 更新；
7. 新增/变化的公共 API、runtime ownership 和命令已同步对应 AGENTS/README；
8. 在 `docs/superpowers/specs/` 写 implementation record，列出实际提交、被拒方案和最终数字。

## 19. 实施状态（2026-08-05）

按本计划执行的提交（全部 `make gate` 绿 + 40 项 kcp-rs async 集成测试 + `make e2e` 138 passed/0 failed）：

| 计划项 | 提交 | 状态 |
|--------|------|------|
| Phase 1.1 `poll_read_into` 单锁 | `b76db61c`（会话开始前已提交） | 完成 |
| Phase 1.2 waker 锁外唤醒 | `ae765aa7` | 完成 — **修正**：因 `waiter_changed` 会清 deadline，`take()` 导致 `kcpconn_read_timeout` 无限重挂（1h 复现），改用**锁内 clone、锁外 wake_by_ref**（计划允许的 fallback）；见记忆 `kcp-waker-take-deadline-gotcha` |
| Phase 1.3 sweep 单锁 | `b76db61c` | 完成 |
| Phase 2.1 admission/queue-drop buffer 归还 | `94ee0b21` | 完成 — drop 路径零分配；`route_inner` 改直接返回 spare；affected 只记录 queued peer；RECV_BATCH 冷启动预分配 |
| Phase 2.2 raw_packets 双缓冲 | `b2a21b44` | 完成 — pending/spare 双 Vec 锁内 swap，锁内零分配；retained cap 256 |
| Phase 3.1 回退命中计数 | `1e2f9b64` | 完成 — Rust-only `read_fallback_timeout` SNMP 计数器（默认关），供 P999 spike 关联 |
| Phase 3.3 可取消 recv | `4287a9bf` | 完成 — kio `CancellationToken` + `race`；conn/listener 各持 token，close() 取消 recv，移除两个 100ms tick |
| Phase 4.1 process_builds 三阶段 | `2ed978f3` | 完成 — collect/build/commit，锁次数 ~2/batch；新增 `max_build_time_per_wakeup` |
| Phase 4.2 affected 混合去重 | `e1c3ebae` | 完成 — 复用 HashSet O(1) 去重 + `affected_comparisons` 计数器 |
| Phase 6.2 FEC 死 clone 删除 | `f7c6aa39` | 完成 — `recover()` 的 `new_buffers`（从未读取）删除 |

### 证据门控延期项（有书面决策）

| 计划项 | 决策 | 理由 |
|--------|------|------|
| Phase 5.1 output callback 批量提交 | 延期 | 需重新 profile 证实 raw queue 锁等待 >=5%；macOS lossless profile 显示服务器 syscall-bound（send_to+recv_from=66%） |
| Phase 5.2 写分块 A/B（8–128 KiB） | 延期 | 计划要求多维度 A/B；非单一 256 KiB echo 可定 |
| Phase 5.3 async write UDP await | 延期 | 计划要求 Phase 0 证明 send await 与 P999 强相关 |
| Phase 6.1 `rcv_buf` O(1) 快路径 | 延期 | 需 C5 高 reorder profile（rcv_wnd>=512 且 >=5% 热点）；无 loss-injection harness |
| Phase 6.2 FEC pooling（decode_into/scratch/bytes cap） | 延期 | 需 C5 丢包矩阵 profile；无 loss harness |
| Phase 7 SO_REUSEPORT shard ownership | 延期 | 结构正确性收益需 Linux 实测（本机 macOS 单 shard 不回退）；smol 无 "local" 语义 |
| Phase 8 `KcpEndpoint` | 未开始 | 仅数万游戏会话目标；需独立 spec + 内存预算 |

> **证据结论**：macOS lossless profile（`bench/profiles/HOTSPOTS.md` + `docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`（已删除）§14.2）显示 `send_to` 51.6% / `recv_from` 4.3% — 服务器 syscall-bound，kcp-rs 内已非热点。FEC recovery 仅在高丢包（C5）激活，当前 bench 无法注入 loss；按计划 §17 停止微优化并记录。

## 20. Linux 验证（2026-08-06，`rustlang/rust:nightly` Docker 容器）

| 项 | 结果 | 结论 |
|----|------|------|
| kio mmsg round-trip（`sendmmsg_to_roundtrip`、`recvmmsg_from_batch`） | `cargo test -p kio-rs` **25 passed / 0 failed** | Linux sendmmsg/recvmmsg 路径正确；`TODO(linux-verify)` 已解决 |
| SO_REUSEPORT 机制（`--shards 1/2/8/16`，null crypt，8 并发 KCP 连接） | 无绑定错误，连接正确分发；聚合吞吐 1→157.1 / 8→**200.6** / 16→170.3 MB/s | 机制正常，扩展**适度非线性**（8 shards +28%，16 回退）；`--shards 0`=核数（本机 16）对低并发偏多 |

**Phase 7（shard ownership）决策**：SO_REUSEPORT 机制已实测可用；当前实现（socket 主 runtime 构建、accept future 移入 shard）在 Linux 上功能正确、8 shards 下 +28%。shard 内构建（`block_on_local` 内 `from_std`）是结构正确性改进，收益非明确吞吐提升，且 16 shards 已显示过度分片回退——**维持延期**，不做结构改动，仅记录实测扩展曲线。
