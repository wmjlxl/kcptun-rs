# Plan: KcpListener 多线程高并发优化（P0.2–P1.3）

> **Canonical path (git):** `docs/superpowers/plans/2026-08-05-KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`

| Field | Value |
|-------|-------|
| Status | implemented |
| Created | 2026-08-05 |
| Scope | kcp-rs listener/transport/conn 的资源有界 + 零分配批量收包 + 每 burst 一次锁 + 批量接收栈；Go wire 兼容性交叉验证 |
| Out of scope | P0.3 执行器归属、P1.4 可取消收包、P2.2 发送批量化（见 `docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`（已删除）§13） |
| Related | `docs/KCP_LISTENER_CONCURRENCY_OPTIMIZATION.md`（v3 审核版，详细设计；已删除）、`kcp-rs/src/listener.rs`、`transport.rs`、`conn.rs`、`kio-rs/src/net/*`、`kcptun-common/src/kcp_transport.rs` |

## Problem

共享 UDP 解复用 listener 在无界资源、逐包锁、逐包 syscall 下无法支撑网络游戏/隧道数据面：
内存 DoS、reader 单点吞吐、跨线程锁竞争。

## 方案（Solution）

1. **P0.2 资源有界 + 生命周期**：`KcpListenerLimits`（默认有界）、`PeerState::{Building,Ready}` + generation 防呆、drop-tail 有界队列、pending 有界、超时自动清理、stats 观测。
2. **P1.1 零分配批量收包**：kio `try_recv_batch_from_into`（Linux recvmmsg 写调用者 slots + 复用 peers Vec）；listener 接入，路由时 `PeerQueue` spare 原地回填槽位。
3. **P1.2 每 burst 一次 sessions 锁** + `max_drain_packets` 预算（v3 §4.2/§5.1）。
4. **P1.3 批量接收栈**：`PeerQueue::pop_batch`、`PeerTransport::try_recv_batch`、`CryptoTransport::try_recv_batch`（坏包稳定压缩）、input loop `try_recv_batch` + `MAX_INPUT_BATCH` 预算。
5. **P2.1（部分）**：`KcpConnBuilder::buffer_size`。

## 实施顺序

P0.2 → P1.1 → P1.2 → P1.3 → P2.1，每步 `make gate`（fmt + workspace test + clippy -D warnings）+ Linux 交叉编译。

## 验收

- `make gate` 全绿
- `make e2e` Go↔Rust 交叉验证 138/138
- Linux `cargo check --target x86_64-unknown-linux-gnu` 通过
