# kcptun-rs 审计与验收报告

日期：2026-10-16（Asia/Shanghai）。

基线：`main` / `c937ebe3`。开始审计时工作区干净；先发现缺陷，随后按第 6 节修复并复验。
结论：**初审发现两处已确认 bug；修复后两处回归与 Go 互通验收通过**。这证明所审缺陷已经修复，不能推导全仓不存在其他 bug。Linux tcpraw 运行时仍未在本机验证。

## 1. 审计范围与证据等级

范围是当前九 crate 工作区的协议主链：UDP/TCP raw、加密与 MTU、FEC/KCP、Snappy、SMUX、客户端会话退休，以及诊断服务的关键边界。重点复核上一轮审计列出的 P1 项和最新提交的修补路径。初审阶段没有改业务代码、公共 API 或 wire 格式；后续修复仅修改上述缺陷的相关路径，未改变 wire 格式。

使用 GitNexus 1.6.12 的本地索引。初始索引停在 `3552b46`，运行 `node .gitnexus/run.cjs analyze --index-only` 后同步至 `c937ebe`；`status` 显示覆盖的 329 个文件内容与当前工作区一致。索引含 7,650 节点、19,768 边、537 条流程。用 `query` 定位会话、FEC、Snappy、tcpraw 路径，用 `context` 与 upstream `impact` 看依赖，再逐段核对源码及运行实测。GitNexus 对 `feed` 等常见方法名的结果有大量名字回退匹配，且标注动态分派边界；其风险等级用于安排核查，不作为缺陷严重度或完整调用者计数。

证据等级：**实测复现**表示构造输入运行了当前代码；**源码确认**表示可从实现和调用链确定条件，但未做目标平台复现；**检查通过**仅表示本次执行的命令通过。

## 2. 已确认缺陷

### F-1（P1）合法高压缩率输入可关闭会话

- 位置：[`snappy_frame.rs`](../kcptun-common/src/snappy_frame.rs#L3-L13) 的 `MAX_FEED_OUTPUT = 1 MiB`；[`snappy_frame.rs`](../kcptun-common/src/snappy_frame.rs#L129-L151) 在同一次 `feed` 中累计解码结果，超过该值报 `InvalidData`。
- 可达路径：[`kcptun_session.rs`](../kcptun-common/src/kcptun_session.rs#L393-L486) 每次最多读取 64 KiB 压缩字节，然后一次性交给解码器；`feed` 出错会跳出循环并关闭 SMUX/KCP。[`snappy_pipe.rs`](../kcptun-common/src/snappy_pipe.rs#L104-L130) 也使用同一个解码器并传播错误。
- 复现：用 `snap::write::FrameEncoder` 编码 17 个各 65,536 字节的零块，原文共 **1,114,112 B**，合法帧压缩后 **52,455 B**，可落在一次 64 KiB KCP 读取中。当前 `SnappyStreamDecoder::feed(&encoded)` 返回 `Snappy decoded output exceeds its limit`。独立临时工程位于 `/tmp/kcptun-audit-snappy`；运行 `cargo run --offline --manifest-path /tmp/kcptun-audit-snappy/Cargo.toml` 得到上述数字和错误。
- 影响：普通可压缩流量若在一次读取中聚合足够多的 Snappy 块，会被误判为坏帧并断开会话。触发依赖读取分批方式，因此既有短消息/单块测试可以通过。
- 修复验收：保留有界内存，但按块向上层输出或在安全预算内分批消费，不把协议合法性绑定到一次 `read` 的聚合量。增加“17 个最大原文块压缩后一次 `feed`”回归用例，并验证 `KcptunSession` 与 `SnappyPipe` 均可完成传输。

### F-2（P1，`null` 模式尤其明显）Snappy 帧 CRC 未校验，损坏数据可交付

- 位置：[`snappy_frame.rs`](../kcptun-common/src/snappy_frame.rs#L121-L154) 对压缩块和未压缩块都跳过前 4 字节校验码，解压/复制后直接输出；没有 CRC32C 比较。
- 复现：将 `FrameEncoder` 产生的合法未压缩帧的最后一个数据字节改 1 bit，保留原 CRC。当前解码器返回被改动的 16 字节数据；标准 `snap::read::FrameDecoder` 对同一帧返回 `Checksum` 错误。只改 CRC 字段时，当前解码器仍接受，标准解码器同样拒绝。
- 影响：Snappy 帧本身不再提供损坏检测；在无外层认证的 `null` 模式可将损坏内容交给 SMUX/应用。AES-GCM 的外层认证可挡住未重新认证的篡改，但不修复 Snappy 解码契约；CFB 外层 CRC 也不能替代帧内校验。
- 修复验收：按 Snappy framing 的 masked CRC32C 验证**解压后的**数据（压缩/未压缩块均覆盖），校验失败返回 `InvalidData`；增加 payload 与 CRC 各翻转 1 bit 的测试，以及合法帧跨 `feed` 分片测试。

## 3. 条件性风险（尚未认定为生产故障）

[`TcpRawConn::raw_send`](../knet-rs/src/net/tcpraw.rs#L404-L464) 用原子 `load(seq)`、`sendto`、`store(seq + len)` 三步更新 TCP 序号。两个线程直接并发调用公开的 `send`/`send_batch_to` 时，可能读到同一序号并交错发包。当前 KCP 主路径有 [`is_sending` 单发送者令牌](../kcp-rs/src/conn/endpoint.rs#L532-L538)，因此没有证据表明二进制主路径会触发；但公开 `TcpRawConn` 的并发调用契约尚未验证。若该 API 承诺并发发送，应将发包与序号推进串行化，并在 Linux 权限环境下测双发送者与部分失败。

GitNexus `check --cycles` 报告 `conn.rs`、`transport.rs`、`sharded.rs` 之间 2 个文件依赖环。Rust 编译和测试均通过；这属于结构维护信息，不是本次认定的运行时 bug。

## 4. 既有高风险修补的核查

| 路径 | 当前证据 | 本次结论 |
|---|---|---|
| SMUX 接收计数/回收 | `push_data_bytes` 在 recv 锁内校验、入队和记账；`recycle_tokens` 关闭入站；并发 push/read/close 测试通过 | 所审交错回归通过，未作长时间压力证明 |
| `cpu_block` panic | worker 的任务闭包 `catch_unwind`，结果在等待侧 `resume_unwind`；panic/取消隔离单测通过 | 所审故障路径通过 |
| Linux `recvmmsg` | 内核写入前保持 Vec 长度为 0，只为有效返回槽 `set_len(actual)`；Linux musl 目标编译通过 | 静态核查通过，未在 Linux 运行时验收 |
| 加密 MTU | shared stack、client/server 入口均调用 `validate_encrypted_mtu`；边界单测通过 | 所审 2048 B 接收槽超长配置已被拒绝 |
| 会话退休 | 按当前 HEAD 重建 release client/server 后，三阶段 `session_retirement_test` 通过 | 旧流完成、新连接换会话、旧会话后回收均通过 |
| FEC auto-tune | 新增环形样本复用和收敛测试；KCP/FEC 全特性测试通过 | 未见本轮回归，Go 互通未实测 |
| pprof 请求限制 | 连接处理任务化，有连接数、头部、body 和总请求期限 | 源码核查；本次未做慢请求/并发 profile 专项压测 |

## 5. 初审执行记录与验收边界

| 检查 | 结果 |
|---|---|
| `cargo test --workspace --all-features` | 通过；部分标记 ignored 的重型测试未执行 |
| `cargo fmt --all -- --check` | 通过 |
| `make clippy`（`-D warnings`） | 通过 |
| `cargo check --target x86_64-unknown-linux-musl -p knet-rs -p kcp-rs --features kcp-rs/async` | 通过；仅编译，不等于 Linux 运行时验证 |
| `cargo build --release -p kcptun-client -p kcptun-server` | 通过，避免用旧 release 二进制验收 |
| `cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture` | 通过，三阶段验收均满足 |
| `make stress` | 通过：8 passed、0 failed、2 ignored；覆盖 10/50/100 并发连接、1 MiB 单连接、可压缩数据等 |
| `make e2e` / `test_e2e.sh` | 初审未运行；项目 AGENTS.md 要求用户明确确认后才能执行 |

本机是 macOS；初审时 Linux mmsg/tcpraw 运行时、权限相关路径、Go↔Rust 互通矩阵不在实测范围。初审时现有通过的测试不能推导“全仓无 bug”；两项 Snappy 复现足以使当时的无已知 bug 验收失败。

初审阶段仅新增审计文档，无业务代码或 API 变更；后续改动见下节。

## 6. 修复与复验（同日追加）

### 修复范围

| 项目 | 修复 | 复验依据 |
|---|---|---|
| F-1 合法高压缩率输入 | 删除单次 `feed` 聚合输出上限；解码器按 64 KiB 输入片处理，按 Snappy 块向会话 SMUX 输出。`SnappyPipe` 每次消费至多 8 KiB 压缩输入，限制单次待返回的解码数据量。 | 17 个 64 KiB 零块组成的合法帧（压缩后小于 64 KiB）在解码器及 `SnappyPipe` 各有回归测试通过；双向 Go↔Rust 大数据传输各通过。 |
| F-2 CRC 漏验 | 对压缩和未压缩数据块按 Go Snappy 的 masked CRC32C 校验解码后的数据；校验或解压失败返回 `InvalidData`。 | 两种块类型的 CRC 翻转均被拒绝；未压缩块 payload 翻转被拒绝；Go↔Rust 默认压缩互通通过。 |
| 修复过程中的 SnappyPipe 回归 | 首版 8 KiB 分批读取在尚未形成完整块时误丢弃同一次底层读取的剩余字节。增加 `snappy_pipe_keeps_unfinished_chunk_within_one_read`：修正前 16 KiB 合法帧读取超时，修正后继续消费剩余字节并完整返回。 | 定向测试先失败再通过；见下方最终检查。该回归未保留在最终代码中。 |
| tcpraw 直接并发发送条件性风险 | `TcpRawConn::raw_send` 增加连接级互斥，从序号读取到 `sendto` 和序号推进保持串行。 | Linux musl 目标编译通过；macOS 无法执行 Linux raw TCP 运行时测试，故不宣称运行时验收。 |

Go client/server 从 `/Users/yangzhiqin/Documents/Project/kcptun` 的当前源码（`42eb6b0`）使用 vendored 依赖重新构建到 `tests/kcptun-go/`，该 Go 源码仓库未被本次修改。Rust client/server 也按修复后的源码重建 release；避免旧二进制造成误判。

| 修复后检查 | 结果 |
|---|---|
| 修复前定向回归 | 新增的高压缩率及损坏 CRC 测试先失败，确认复现原缺陷 |
| `cargo test -p kcptun-common --all-features -- --test-threads=1` | 最终版本 74 通过、0 失败；较早一次并行重跑有 2 个时延断言失败，一次单线程重跑仍有 1 个超时，随后隔离该测试及完整单线程重跑均通过。按时序波动记录，不把单次通过写成稳定时延保证 |
| `cargo test --workspace --all-features` | 通过；少数标记 ignored 的用例未运行 |
| `cargo fmt --all -- --check`、`make clippy`、`git diff --check` | 最终版本通过 |
| `cargo check --target x86_64-unknown-linux-musl -p kcptun-common --all-features` | 通过；仅编译验证 |
| `go test -mod=vendor ./...`（Go 源码仓库） | 通过 |
| `cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture` | 通过；旧流、新会话和旧会话回收三阶段均满足 |
| Go↔Rust 大数据双向互通 | 最终 release 二进制中，`aes`、`null` 各双向传输并校验回显 1,179,648 B，共 4 组通过 |
| `bash test_e2e.sh` | SnappyPipe 最终小修之前的生产会话路径：68 通过、0 失败；覆盖全部 15 种加密算法的双向无压缩互通，以及模式、SMUX、压缩、FEC。tcpraw 因 macOS 非 Linux/root 被脚本提示跳过；脚本计数器未计入此跳过项。最终小修未修改生产会话 `read_loop` |
| `make stress` | 8 通过、0 失败、2 个重型用例按原标记 ignored；包括 10/50/100 并发连接、单连接 1 MiB、多端口和可压缩数据完整性 |

GitNexus `detect-changes --scope all` 在最终修复后识别 9 个文件、22 个符号的图变更，影响主会话执行路径并给出 `critical` 风险。源码核查证实这是主链路变更，不能用图的名字回退边当作精确调用者数量。上述回归、工作区测试和 Go 双向互通用于验收其实际行为；图分析不代替运行时测试。

更新了 `kcptun-common/AGENTS.md` 和 `knet-rs/src/net/AGENTS.md`，记录新的解码和 raw send 约束。`SnappyPipe` 的测试模块已按仓库约定移到相邻的 `snappy_pipe/tests.rs`，测试内容保持原样并增补两项回归。**剩余边界：**本机无法验证 Linux raw TCP 并发发送、mmsg 实际收发和权限相关行为；本报告不提供“全仓无 bug”的绝对保证。
