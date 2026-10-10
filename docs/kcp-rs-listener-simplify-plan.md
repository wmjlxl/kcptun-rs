# kcp-rs 独立网络库架构方案

状态：方案（未动代码）
日期：2026-09
定位：**kcp-rs = 可单独依赖的高性能 KCP 网络库**，不再按 kcptun 附属模块设计。
前置讨论：多 worker 必要性 → 自建线程池/三拓扑裁剪；本文件按新约束（独立库 / 高并发 / 多核 / 零系统线程）重写目标形态。

---

## 0. 设计约束（硬）

| # | 约束 | 含义 |
|---|------|------|
| C1 | **独立网络库** | 不依赖 kcptun-*、不为隧道场景特化；API 面向通用 UDP 可靠传输 |
| C2 | **高并发** | 万级～十万级会话；会话常驻成本可预算、可限额 |
| C3 | **高性能** | 批量收发、少跳数、少分配；保留既有热路径成果 |
| C4 | **充分利用多核** | 单 socket 也要能吃满多核（不能 1 会话泵 = 1 核） |
| C5 | **尽量不手动开系统线程** | 库内禁止 `std::thread` / `thread::Builder` / 自建 runtime；只用调用方 tokio 的 **task** |

推论：

- 多核并行单元 = **tokio task**（由 runtime work-steal 到 N 核），不是 OS 线程。
- 单 socket 下不能只靠「一个收发循环」并行 —— 必须把**会话级工作**拆到多个 task。
- 不能假设库拥有进程；不能 `block_on_*` 开私有 runtime（那是应用的事）。

---

## 1. 现状与冲突

| 现状 | 与约束冲突 |
|------|------------|
| `sharded.rs`：`thread::Builder` + 每 worker 私有 `block_on_local` / reader `block_on_multi_thread` | 违反 C5 |
| 三拓扑 + FNV + channel worker 池 | 复杂度高，且并行单位是线程 |
| Mode B：单 worker 内联 decrypt+KCP+encrypt | 单 socket 时整条热路径钉在 1 个 task = **1 核**，违反 C4 |
| `from_socket` 默认 `num_cpus()` worker → reader 管线 | 生产双重分片；对库用户是隐式线程爆炸 |
| `PeerQueue` 入队路径已死，`background_input(true)` 才是「会话自己收」 | 其实更符合 C4，被 Mode B 旁路了 |
| `kcptun-server` app 层 `--shards` + 自建线程 | 是**应用**的事；库应让单进程单 socket 就能多核 |

结论：Mode B / worker 池是 kcptun 延迟实验的产物，**与「独立库 + 多核 + 零线程」目标相反**。库默认路径应改为 **会话任务化**；Mode B 降级为可选优化（甚至删除）。

---

## 2. 目标形态（库默认）

```text
┌────────────────────────────────────────────────────────────┐
│        调用方 tokio runtime（multi-thread，库不创建）        │
└────────────────────────────────────────────────────────────┘
         │ spawn_task only（零 std::thread）
         ▼
┌─────────────┐   recvmmsg 批量    ┌──────────────────────────┐
│  rx task    │ ─────────────────► │ SessionMap               │
│  (每 socket │   peer + datagram  │  SocketAddr → SessionHx  │
│   一个)     │                    └──────────────────────────┘
└─────────────┘                            │
     │                                     ▼
     │ 新 peer：准入门禁              ┌──────────────────────────┐
     │  → build KcpStream            │ Session inbox (有界队列) │
     │  → accept 队列                │  try_send，满则丢+计数   │
     │                               └────────────┬─────────────┘
     ▼                                            ▼
  用户 accept()                     ┌──────────────────────────┐
                                    │ session task × 会话数    │
                                    │  recv inbox 批量         │
                                    │  → feed → KCP → ACK/flush│
                                    │  → 用户 AsyncRead/Write  │
                                    │  + flush 定时器          │
                                    └──────────────────────────┘
         多核：session tasks 被 work-steal 到 N 个 runtime worker
         可选：SO_REUSEPORT × M 个 rx task（极高 pps 时扩 RX）
         可选：pump 分片（十万会话时避免 task 爆炸）→ 见 §4.3
```

### 2.1 会话模型（默认）

```text
KcpListener                    KcpStream
───────────                    ─────────
rx: demux + 建连               session task: 收 inbox → feed_raw
accept 队列                    flush loop（tokio 定时器）
限额 / reaper                  AsyncRead/Write（用户侧）
```

- **每会话 1 个主 task**（输入+flush 合一）或 **2 个 task**（输入 / flush 分离，复用现有 `background_input(true)`）。
- 同一会话的包在同一 task 内串行 → 无 `Mutex<KCP>` 跨核抢锁、无乱序。
- 不同会话在不同核并行 → C4。
- 会话空闲时阻塞在 `inbox.recv()` / 定时器上，成本 ≈ 1 个 tokio task + 1 条队列。

### 2.2 多核策略（按优先级）

| 层级 | 机制 | 何时 |
|------|------|------|
| 1 | **session task work-stealing**（默认） | 全时；单 socket 即可多核 |
| 2 | **SO_REUSEPORT × M rx task**（可选 builder） | RX 或 sendmsg 成瓶颈的极高 pps |
| 3 | **pump 分片**（可选 builder） | 会话数 ≫ 1 万，task 调度/内存成为瓶颈 |

**禁止**：库内 `thread::spawn`、自建 runtime、`block_on_local/multi_thread`。

---

## 3. 公开 API（库面向，0.3.0）

```rust
// —— 监听（服务器）——
let listener = KcpListener::bind("0.0.0.0:29900")
    .mtu(1400)
    .mode(KcpMode::Fast3)
    .fec(10, 3)                 // 可选
    .max_sessions(100_000)      // 0 = 不限（不推荐）
    .idle_timeout(Duration::from_secs(600))
    .accept_backlog(1024)
    // 可选扩展（默认关闭）：
    .reuseport(4)               // Linux：M 个 socket，内核 4-tuple 分流
    .pumps(0)                   // 0=每会话一 task；N=hash 分到 N 个 pump task
    .build()
    .await?;

let (stream, peer) = listener.accept().await?;
// stream: AsyncRead + AsyncWrite + KCP 调优 set_kcp_*

// —— 拨号（客户端）——
let stream = KcpStream::connect("1.2.3.4:29900")
    .connect_timeout(Duration::from_secs(3))
    .build()
    .await?;

// —— 传输抽象（加密等由上层包一层）——
let stream = KcpStream::with_transport(transport, cfg).await?;
```

### 3.1 相对 0.2.x 删除 / 变更

| 项 | 处理 | Breaking |
|----|------|----------|
| `builder.worker_count` / `listener.worker_count` | **删** | 是 |
| `KCPTUN_WORKER_THREADS` | **删** | 是（env） |
| `WorkerPoolLimits.worker_channel_cap` | **删**（改为 `session_inbox_cap` 语义并存可选） | 是 |
| `WorkerPoolStats.channel_drops` | **改名** `inbox_drops` | 是 |
| `bind_listener` / `from_socket_listener` | 保留 thin alias 或删（推荐保留） | 否/是 |
| `thread` 相关行为（私有 runtime） | **删**，任务进调用方 runtime | 行为变化 |
| Mode B / `background_input(false)` + 外部 `feed_raw_batch` | **保留为 advanced API**，默认走会话 inbox | 否（默认路径变） |
| `KcpTcpListener` | 另 PR；与 UDP 库主路径无关 | 可选 |

`WorkerPoolLimits` 收成库风格：

```rust
pub struct KcpLimits {
    pub max_sessions: usize,          // 默认 0=不限，文档强烈建议设置
    pub session_inbox_cap: usize,     // 默认 2048；满则 drop + SNMP
    pub accept_backlog: usize,        // 默认 1024
    pub building_timeout: Duration,   // 默认 10s
    pub idle_timeout: Duration,       // 默认 600s；KCP 无 keepalive
}
```

---

## 4. 内部模块裁剪

### 4.1 删除（与 C5 / 简化直接相关）

| 项 | 位置 | 约行数 |
|----|------|------:|
| `std::thread` / `thread::Builder` / `JoinHandle` 线程池 | `sharded.rs` | ~100 |
| `block_on_local` / `block_on_multi_thread` 私有 runtime | `sharded.rs` | ~30 |
| `WorkerRx::{Channel, Direct}` 双拓扑 + `Topology` | `sharded.rs` | ~120 |
| FNV `fast_hash_peer` 用户态分片（默认路径） | `sharded.rs` | ~30 |
| `spawn_sharded_reader`（线程版 RX） | `sharded.rs` | ~110 |
| `spawn_worker` 线程事件循环（Channel/deferred/预算双份） | `sharded.rs` | ~350 |
| `num_cpus` / `KCPTUN_WORKER_THREADS` | `sharded.rs` | ~20 |
| `worker_count` API | `sharded.rs` | ~15 |
| 自建 reuseport **组**逻辑（改由 `reuseport(n)` 显式、内部只开 socket+task） | `sharded.rs` | 含上 |

预估净删 **700–900 行**（`sharded.rs` 1972 → 约 1100，含测试；新 session 路径 +150～250）。

### 4.2 保留并扶正

| 项 | 原因 |
|----|------|
| **`PeerQueue` / 会话 inbox 入队路径** | 从「legacy 将死」改为**默认 RX→session 投递** |
| `background_input(true)` 会话自收（或等价 session task） | 多核 work-stealing 的载体 |
| `feed_raw_batch` / `background_input(false)` | 保留给 pump 模式 / 低延迟调用方（advanced） |
| 批量 `recvmmsg`（rx task） | C3 |
| 预准入 decrypt 门禁 | 防伪造占槽 |
| idle / building reaper、`remove_peer` | C2 资源治理 |
| conv-mismatch 陈旧会话驱逐 | 正确性 |
| `transport_wrapper` / `PacketTransport` | 库可插拔加密 |
| `drain_and_flush_tx` 非阻塞发送 | C3 |
| `KcpStream` 的 TcpStream 对齐表面 | C1 通用库体验 |

### 4.3 高并发两种会话驱动模式（builder 可选）

**A. 每会话 task（默认）** — `pumps(0)`

- rx → `inbox.try_send` → session task `recv` 后 `feed`
- 多核：work-stealing；同会话串行
- 容量：~1e4–5e4 会话舒适；1e5 需压测内存/调度
- 最符合 tokio 心智；公平性好

**B. pump 分片** — `pumps(N)`，N 默认 `available_parallelism().min(16)`

- rx → `fast_hash(peer) % N` → pump inbox → pump task 内联 Mode B `feed_raw_batch`
- 多核：N 个 pump task ≈ N 核；同会话钉在同一 pump（缓存友好）
- 容量：会话进 HashMap，不进 task；适合 1e5+ 或延迟敏感
- 这是「worker」的正确形态：**N 个 task，0 个系统线程**

> 默认 A：通用库优先简单与公平；B 是给高并发/低延迟的显式旋钮，**不是**隐藏默认。

### 4.4 SO_REUSEPORT（可选）

`reuseport(n)`：创建 n 个 UDP socket（Linux `SO_REUSEPORT`），n 个 rx task；会话仍按 §4.3 A/B 驱动。  
非 Linux：`reuseport(n)` 返回错误或退化为 1 socket（文档写明）。  
**库不再自动按核数开 socket**；调用方显式选择。

---

## 5. kcptun-server 如何用（迁出库）

```text
// 每个 --shards fd（应用已建好 SO_REUSEPORT）：
KcpListener::from_socket(udp)
    .limits(KcpLimits { max_sessions: 4096, .. })
    .transport_wrapper(|t, _| Arc::new(CryptoTransport::wrap(t, key, crypt)))
    .pumps(1)                 // 每 fd 一个 pump = 应用层 --shards 负责多核
    .build()
    .await?
```

- 应用保留 `--shards` + 自建 shard 线程/`block_on_local`（那是**应用**的线程策略）。
- 或更干净：去掉 app 线程，单 runtime + `.pumps(available_parallelism())` 或 `.reuseport(N)`，库负责多核。
- **Phase 0 止血不变**：`from_socket` 默认单管线，避免双重分片。

---

## 6. 分阶段迁移

### Phase 0 — 止血（独立 PR，~20 行）

1. `from_socket` 默认 1 条管线 / `kcptun-server` 显式 `.worker_count(1)`。
2. 回归：默认不再起 `kcp-reader` + N×`kcp-worker` 线程。
3. 基线：`run_p99.sh` / `run_bench.sh` 存数。

### Phase 1 — 零线程 + 会话任务化（主刀）

1. 删 `std::thread` 全部用法；`spawn_worker` → `spawn_session` / `spawn_rx`（`knet::spawn_task`）。
2. 复活 `PeerQueue`/inbox 为**默认** RX 投递；`background_input(true)` 为默认会话驱动。
3. `background_input(false)` + `feed_raw_batch` 保留，文档标 advanced（pump / 外部调度）。
4. 删三拓扑、FNV 默认分片、`worker_count`、`KCPTUN_WORKER_THREADS`。
5. `WorkerPoolLimits` → `KcpLimits`（更名 + 字段语义对齐 §3.1）。
6. reaper 改为 listener 级 `spawn_task` 定时器（不绑私有 runtime）。
7. 测试：去 `worker_count(n)`；补「万会话 spawn / 关闭无泄漏」「单 socket 双核并行」烟测。
8. 文档：`kcp-rs/AGENTS.md`、README、根 AGENTS 中「三拓扑/worker 池」段落替换为本架构。

验收：

```bash
cargo test -p kcp-rs --features async
cargo test -p kcp-rs --all-features
make clippy
# 零线程断言（可加测试）：kcp-rs 代码路径无 std::thread::spawn
# 多核烟测：单 socket、S=2 核、N 会话并行吞吐 ≈ 1.5–2× 单核（粗验 C4）
# 高并发烟测：2e4 会话 accept/close 循环，无 task/socket 泄漏
```

### Phase 2 — `pumps(n)` + `reuseport(n)`（显式扩展）

1. `pumps(n)`：hash 分片 pump task，内联 `feed_raw_batch`（Mode B 只活在这里）。
2. `reuseport(n)`：多 socket 多 rx task。
3. bench 对比：`pumps(0)` vs `pumps(N)` vs `reuseport(N)` 的 rps / P99 / 内存。

### Phase 3 — 性能打磨（有数据再做）

1. inbox 合批、buffer pool、timer wheel 合并。
2. `spawn_local` + `LocalSet` 钉 pump（可选亲和）。
3. 极限 pps 的 RX 水平扩展。

### 明确不做

- 库内 OS 线程 / 私有 runtime / `block_on_*`
- 自动 `num_cpus()` 隐式并行（一切并行显式：默认会话 task；`pumps`/`reuseport` 可选）
- 默认退回「单循环整核 Mode B」
- wire 格式 / KCP 状态机 / FEC / 加解密布局变更

---

## 7. 风险

| 风险 | 缓解 |
|------|------|
| 会话 task 数量大（1e5）内存/调度压力 | 默认 A + 文档给上限；`pumps(n)` 提供 B；`max_sessions` 硬顶 |
| rx 单 task 成为 pps 瓶颈 | 批量 recvmmsg；瓶颈实测后用 `reuseport(n)` |
| 同会话若跨 task 会抢 `Mutex<KCP>` | 会话串行：一 session 一 task 或钉在同一 pump（A/B 都保证） |
| 尾延迟较 Mode B 回退 | Phase 1 前后 P99/P999 对比；延迟敏感方显式 `pumps(n)` |
| 0.2.x 公开 API 破坏 | 0.3.0 + CHANGELOG；workspace 内调用点已盘过 |
| kcptun 行为回归 | stress_test +（需确认后）e2e；Phase 0 先保生产 |
| `PeerQueue` 复活引入锁热点 | 入队 `try_send` 无 await；有界；压测 inbox 锁竞争 |

---

## 8. 工作量

| 阶段 | 规模 | 交付 |
|------|------|------|
| Phase 0 | ~20 行 + 测试 | 生产双重分片修复 |
| Phase 1 | 删 ~800，增 ~200，改测试/文档 | **零线程库默认架构** |
| Phase 2 | +200～300 | `pumps` / `reuseport` |
| Phase 3 | 按数据 | 极限性能 |

---

## 9. 一句话决策

**kcp-rs 默认「rx task + 会话 task（work-stealing 多核）」，库内零系统线程；高并发用有界 inbox + 限额；极高并发/低延迟用显式 `pumps(n)`（task 级 Mode B）或 `reuseport(n)`；彻底去掉自建线程池、三拓扑和隐式 worker 数。**
