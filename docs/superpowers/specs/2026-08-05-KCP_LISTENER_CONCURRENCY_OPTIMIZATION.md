# Spec: KcpListener 并发优化（P0.2–P1.3 + P2.1 部分）— implementation record

> **Canonical path (git):** `docs/superpowers/specs/2026-08-05-KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-05 |
| All commits | single session (uncommitted at spec write time) |
| Bug report | none |

## 改动的文件列表（Files changed）

| File | Change |
|------|--------|
| `kcp-rs/src/listener.rs` | `KcpListenerLimits`（默认有界）、`PeerState::{Building,Ready}` + generation 防呆、`PendingAccept`、`ListenerStats`、`ListenerCtx::{route_inner,process_builds,sweep}`、`spawn_listener_reader`（一次 sessions 锁 + 零分配 recvmmsg 排空 + `max_drain_packets` 预算）、`stats()/session_count()/pending_count()` |
| `kcp-rs/src/transport.rs` | 有界 `PeerQueue`（drop-tail + `packet_bytes` + `pop_batch` + spare 回收）、`PeerTransport::try_recv_batch`/`supports_recv_batch`、`pop_batch` 顺序/有界单元测试 |
| `kcp-rs/src/conn.rs` | `INPUT_BATCH_GROW`/`MAX_INPUT_BATCH`/`DEFAULT_WRITE_BUFFER`、input loop `try_recv_batch` 批量排空分支（带预算 + 单包 fallback）、`KcpConnBuilder::buffer_size(bytes)` |
| `kio-rs/src/net/mmsg.rs` | `recvmmsg_from_into`（写调用者 peers Vec，零分配；Linux-only） |
| `kio-rs/src/net/{tokio,smol}.rs` | `UdpSocket::try_recv_batch_from_into`（Linux recvmmsg；非 Linux 原地填槽不再 to_vec） |
| `kio-rs/src/net/mod.rs` | `DatagramSocket::try_recv_batch_from_into` 分发 |
| `kcptun-common/src/kcp_transport.rs` | `CryptoTransport::try_recv_batch`（逐槽原位解密 + 坏包稳定压缩）+ `supports_recv_batch` 透传、mock 坏包压缩测试 |
| `docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`（已删除） | 实施状态 §13（已完成/未实施+理由） |

## 修复的故障路径（Fixed failure paths）

- 无界内存 DoS：任意来源首个 datagram 即建 `KcpConn` 且无清理 → 默认有界 + Building/pending 超时回收 + 准入丢弃。
- reader 单点吞吐：每包 `recvfrom` syscall + 每包两把锁 → Linux recvmmsg 零分配批量收包 + 每 burst 一次 sessions 锁 + 排空预算。
- 连接风暴：reader 内联无界 build → `process_builds` 每 wakeup 预算（Building 队列，generation 防呆）。
- 慢 peer 阻塞：queue 无界 → drop-tail，reader 永不等待。
- 坏密文：`try_recv_vec` 单包循环 → 批量稳定压缩，有效包保序。

## 测试结果（Test results）

- `make gate`（fmt + `cargo test --workspace` + `cargo clippy --workspace -- -D warnings`）：**全部通过 ×4**
- Go↔Rust e2e（`make e2e`，tokio+smol × 全 crypt × mode × SMUX × compression × FEC）：**138 passed / 0 failed ×2**
- Linux 交叉编译：`cargo check -p kio-rs -p kcp-rs -p kcptun-common --features async-tokio --target x86_64-unknown-linux-gnu` 通过
- 新增测试：`pop_batch` 顺序/回收、drop-tail 有界、坏密文夹在有效包之间的稳定压缩

## 修订记录（Revision history）

| Date | Note |
|------|------|
| 2026-08-05 | 初始实施；P0.3/P1.4/P2.2 未实施（见优化文档 §13） |
