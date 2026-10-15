# kcp-rs 代码审查与优化建议

审查日期：2026-10-11。版本：`kcp-rs 0.2.7`。范围：仅 `kcp-rs` 的 KCP 状态机、FEC、异步连接、监听器、缓冲管理、SNMP、文档与测试。其他 crate 仅作为构建依赖使用，没有分析其实现或提出问题。此次交付是分析报告，生产源码未修改。

审查与运行验证最初使用用户复制的完整工作区 `/Users/sean/kcptun-rs-review`。2026-10-11 原项目目录权限恢复后，本报告及复现包已同步到 `/Users/sean/Documents/kcptun-rs/bugs/`。同步前后校验原项目与审查副本内的 26 个 `kcp-rs` 源码、原有测试和 Cargo.toml 文件，逐文件一致；报告中的源码与复现包链接现指向原项目。本次同步未修改生产源码，也没有重新运行测试，下面的运行结果仍来自此前审查。

## 结论与优先级

发现并复现了 3 项应优先修复的问题：异步 trait 的 flush/shutdown 提前返回成功而丢掉尾数据、闲置回收后待接受队列仍持有旧连接、stream send 返回错误后仍将输入前缀入队。另有 10 项配置、边界与生命周期问题，详见下文。P1 表示可能丢失或污染用户数据、或使资源配额失效；P2 表示需要特定模式、配置、时序或输入才能触发。

已有异步测试运行通过，不能据此断言这些边界正确。新增的 22 个检查中，12 个 core/FEC 复现检查（含正常行为对照）与另外 2 个异步对照检查通过，8 个断言预期正确行为的回归测试失败；这里的失败是发现问题的证据。

| 优先级 | 问题 | 复现结果 |
|---|---|---|
| P1 | AsyncWrite flush/shutdown 提前成功 | 成功返回后立即释放 owner，底层发送数据报数仍为 0 |
| P1 | idle reaper 未清理待接受队列 | 会话上限 1，连续回收 3 个 peer 后仍能 accept 出 3 个已关闭连接 |
| P1 | stream send 失败前修改旧 segment | 返回 TooManyFragments，却传输了被拒绝输入的前 25 字节 |
| P2 | message 分片数超过完整接收窗口 | 无丢包模拟 10 秒，33 分片消息一直 NoData，远端窗口为 0 |
| P2 | 默认 feature 缺少同步配置 API | 默认集成测试编译失败，找不到 KcpConfig 与 KCP::apply |
| P2 | listener 内联收包未刷新接收时钟 | 每 100ms 收到探测包，rx_age 仍增长到约 2359ms |
| P2 | 可接受的 MTU 大于收包缓冲 | MTU 4096、3500 字节数据超时；MTU 1400 对照成功 |
| P2 | 非 batch transport 收包没有预算 | 连续读取超过 64 包才处理 ACK；batch 对照有预算 |
| P2 | remove_peer 未取消 building | 移除后旧构建任务仍发布连接 |
| P2 | 非法 FEC 参数静默关闭 FEC | 200+100 shards 的 build 成功，而非 InvalidInput |
| P2 | 缩小 MTU 未处理旧 segment | MTU 从 1400 改为 100，仍发送或重传 1024 字节报文 |
| P2 | RTT 中间算术溢出 | 构造较旧 ACK：debug panic，release RTO 从 200 变成 30ms |
| P2 | FEC 自动探测拒绝 256 shards | 128+128：匹配 decoder 恢复 4 包，自动探测 decoder 恢复 0 包 |

## 已确认问题与修复建议

### 1. P1：AsyncWrite 的 flush/shutdown 提前成功，owner 释放后尾数据丢失

位置：[halves.rs:246](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/halves.rs:246)、[halves.rs:251](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/halves.rs:251)；分割写半部还有同样实现 [halves.rs:397](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/halves.rs:397)。

`poll_write` 将数据入队，`poll_flush` 只通知后台任务便返回 `Ready(Ok(()))`；`poll_shutdown` 关闭写入后同样立即成功。单线程 runtime 中，短写没有产生 Pending 时，`trait write_all → trait flush/shutdown → drop owner` 可以全程不让出执行权。随后连接关闭，发送循环先检查 closed 并退出，已入队的尾数据没有发出。

实测 flush 与 shutdown 两种路径均为 0 个已发送数据报。必须明确限定为 `AsyncWrite` trait 或泛型 I/O 路径：固有的 `KcpStream::write_all` 与 `OwnedWriteHalf::write_all` 有内联发送路径，不能混为一谈。问题也不是简单的“没有等待对端 ACK”。[AsyncWrite 官方约定](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html)要求 flush 完成缓冲刷新，shutdown 包含成功刷新。

建议建立发送完成条件和对应 waker，只有此次 flush 之前的数据交给 transport 后才返回成功；shutdown 先完成该条件，再完成写半部关闭。明确保证到 transport 还是到对端 ACK，不能只靠通知代替完成。保持当前 KCP 无 wire FIN 的设计。

证据：`audit_async.rs` 中 `trait_flush_must_deliver_queued_data_before_drop` 与 `trait_shutdown_must_deliver_queued_data_before_drop`。

### 2. P1：闲置回收释放 session 配额，却保留待接受连接

位置：[sharded.rs:1175](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:1175)、[sharded.rs:1377](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:1377)、[sharded.rs:339](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:339)。

构建完成把拥有连接的 `PendingAccept` 放进 pending 队列。idle reaper 从 sessions 移除连接并关闭，但未移除对应 pending；其他 stale eviction 路径也需要一并检查。accept/try_accept 没有跳过已关闭的连接。新会话因此可以反复占用并释放 sessions 配额，旧连接则继续留在 pending 中。

实测上限为 1、idle timeout 为 20ms，三个不同 peer 依次创建并回收，不调用 accept。最后 sessions/building 均为 0，但仍能取出 3 个已关闭连接。配额没有约束整个生命周期；若应用长期不接收连接，pending 没有硬上限，可继续增长。

建议将 accept backlog 纳入资源预算，为其设置硬上限；所有 eviction 路径同时按连接身份或 generation 移除对应 pending，避免把同地址的新连接误删。accept 跳过 closed 是必要保护，但仅跳过不足以约束无人 accept 时的内存。

证据：`audit_listener.rs::audit_reaper_removes_expired_pending_connections`。

### 3. P1：stream send 返回 TooManyFragments 前已修改队列

位置：[kcp.rs:423](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:423)、[kcp.rs:451](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:451)。

stream 模式先将输入追加到 snd_queue 的最后一个未满 segment，然后才计算剩余输入的分片数。超过 255 时返回错误，却没有撤销已经追加的部分。调用者合理地按失败重试，可能重复发送前缀或污染数据流。

实测 MTU=50、MSS=26，先 send `a`，再 send 长度 `256*26+25` 的 `b`。第二次返回 TooManyFragments，接收端却收到 `a` 加 25 个 `b`。这是公开的底层 `KCP::send` API 问题；异步 facade 的分批写入通常避免这个大输入。

建议在修改队列前计算能够追加的长度、剩余分片数，完成全部校验后再提交；也可明确实现可回滚事务。返回错误时不应产生未告知调用者的部分写入。

证据：`audit_core.rs::error_from_stream_send_still_transmits_rejected_prefix`。

### 4. P2：message 模式接受无法装入接收窗口的完整消息

位置：[kcp.rs:451](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:451)、[kcp.rs:600](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:600)、[kcp.rs:1055](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:1055)。

send 允许最多 255 个分片；默认 bare KCP 的接收窗口只有 32。message 模式需要整条消息齐全才能 recv，rcv_queue 满 32 后窗口归零，第 33 片无法进入队列，又无法消费前 32 片释放窗口，形成持续停滞。

无丢包模拟 10 秒，33 分片消息始终 NoData，接收序号停在 32，发送端远端窗口为 0，未发送队列仍有 1 片。32 分片 message 与 33 分片 stream 对照均成功。`KcpConfig` 默认 stream=true、窗口=128，因此不是所有默认 KcpStream 写入都会触发；显式使用 message 模式时仍有对应边界。

建议定义完整消息上限及重组容量：可以拒绝超出双方约定容量的消息，或让有界重组结构支持所承诺的最大消息。不能把动态变化的剩余 `rmt_wnd` 当成协商好的最大接收窗口。

证据：`audit_core.rs::accepted_message_cannot_fit_default_receive_window` 及对照测试。

### 5. P2：默认同步配置 API 被 async feature 错误隐藏

位置：[lib.rs:37](/Users/sean/Documents/kcptun-rs/kcp-rs/src/lib.rs:37)、[lib.rs:59](/Users/sean/Documents/kcptun-rs/kcp-rs/src/lib.rs:59)、[config.rs:143](/Users/sean/Documents/kcptun-rs/kcp-rs/src/config.rs:143)。

README、crate AGENTS 和同步集成测试把 KcpConfig/KcpMode/KCP::apply 当成同步 API；实际 config 模块及 re-export 受 async feature 控制。完整工作区的默认 `cargo test -p kcp-rs --lib --tests` 编译失败：data_correctness.rs 找不到 KcpConfig，KCP 没有 apply 方法。单独同步 lib 的 56 个单元测试通过，所以应表述为同步配置接口与默认集成测试失效，而不是整个同步库无法构建。

建议将纯配置类型与 `impl KCP` 默认编译，仅异步连接、transport 及相关链接受 feature 控制；验证默认与 async 两个构建组合，修正 test.sh 的首阶段。

### 6. P2：listener 内联收包只刷新 activity，没有刷新 inbound 时钟

位置：[endpoint.rs:399](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/endpoint.rs:399)、[endpoint.rs:250](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/endpoint.rs:250)、[sharded.rs:1210](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:1210)。

已建立 listener 会话由 worker 的 feed_raw_batch 内联处理数据，不再经过通常收包循环中的 mark_inbound。该路径只调用 mark_activity，导致 last_rx_ms 持续变旧。note_io_error 则将接收静默至少 2000ms 作为信任致命 UDP 错误的条件。

实测每 100ms 给活跃会话发送有效 WASK，23 次后 rx_age 仍约 2359ms。直接确认的是时钟异常；结合错误分类路径可推导活跃连接可能被一次传输错误误关闭，此次没有注入该致命错误进行运行验证。

建议在已解包且有效的内联收包路径刷新 inbound 状态与时间，使所有正常接收入口使用统一逻辑；用真正的 listener 路径测试，避免测试直接手动设置时间戳。

证据：`audit_async.rs::listener_inline_receive_must_refresh_rx_age`。

### 7. P2：大 MTU 被接受，但普通收包路径固定为 2048 字节

位置：[transport.rs:29](/Users/sean/Documents/kcptun-rs/kcp-rs/src/transport.rs:29)、[endpoint.rs:883](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/endpoint.rs:883)、[sharded.rs:734](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:734)、[kcp.rs:313](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:313)。

配置只校验 MTU 下限，没有和接收容量保持一致。普通 socket/listener 使用 2048 字节接收缓冲，实际大于此大小的报文会被截断或拒收，重复重传仍然过大。

localhost 实测双方 MTU=4096，发送 3500 字节无法完成，读超时；同样数据 MTU=1400 正常。仅设置大 MTU 但一直发送小数据未必触发；自定义 recv_vec 可以扩展 Vec，因此不能断言所有自定义 transport 都受限。

建议依据 transport 的最大数据报能力做配置验证，或让接收缓冲按有效 MTU 动态分配并设置上限。同时定义 FEC 和外部封装开销的容量契约；此处无需把封装实现放入 kcp-rs。

证据：`audit_async.rs::mtu_4096_must_transfer_large_packet` 与 1400 对照。

### 8. P2：非 batch transport 的收包 drain 没有单轮预算

位置：[endpoint.rs:952](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn/endpoint.rs:952)、[transport.rs:140](/Users/sean/Documents/kcptun-rs/kcp-rs/src/transport.rs:140)。

supports_recv_batch=false 时，循环一直 try_recv_vec，直到 WouldBlock/空/错误才退出；没有 MAX_INPUT_BATCH=64 检查、close 检查或让出执行权。KCP input 和 ACK 在循环结束后才执行。持续 ready 的 transport 可让 burst 增长并阻止协议处理；有限的大 burst 也突破预算。

实测自定义 transport 连续提供 128 包，在尚未处理 ACK 时读取超过 64 包；batch 对照通过。trait 默认 false，标准 DatagramSocket/PeerTransport 则显式支持 batch，所以应限定在 fallback 路径。

建议两条路径共用包数或字节预算，预算耗尽先处理 KCP 并给予其他任务执行机会，再继续收包；限制保留的 burst 缓冲规模。

证据：`audit_async.rs::fallback_input_must_bound_burst` 及 batch 对照。

### 9. P2：remove_peer 没有取消正在构建的同地址连接

位置：[sharded.rs:287](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:287)、[sharded.rs:1134](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:1134)。

remove_peer 清理 pending 和 sessions，却不清理 building。旧任务仍拥有有效 generation，可以在移除之后发布到 sessions 和 pending，违背函数“尚未 accept 的 build 也移除”的注释。

使用现有 testing_build_delay=300ms，在 building_count=1 时 remove_peer，等待后 try_accept 仍返回旧连接。

建议移除 building、关闭其队列并使 generation 失效；同时协调移除、发布与 pending 的竞态，不能只增加一次未受锁保护的检查。return 值也应反映是否取消了 building 状态。

证据：`audit_listener.rs::audit_remove_peer_cancels_building_peer`。

### 10. P2：非法 FEC 参数未报错，构建成功后静默变成无 FEC

位置：[conn.rs:1239](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn.rs:1239)、[conn.rs:1258](/Users/sean/Documents/kcptun-rs/kcp-rs/src/conn.rs:1258)。

FecEncoder/FecDecoder 构造失败返回 None，但连接 build 没有检查并转成错误。200+100 shards 超过上限仍构建成功；更混乱的是 fec_enabled 仍为 true，MTU 仍减去 FEC 头开销。

建议提前校验 shard 数量与总和，明确任一边为 0 时的关闭规则，构造失败返回 InvalidInput。不能让无效配置静默改变协议行为。此处证明的是配置未兑现，并不意味着双方都会永久无法通信。

证据：`audit_async.rs::invalid_fec_configuration_must_be_rejected`。

### 11. P2：运行时缩小 MTU 后仍发送大于新 MTU 的旧 segment

位置：[kcp.rs:313](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:313)、[kcp.rs:1458](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:1458)。

set_mtu 只修改 mtu/mss 与输出缓冲，已入队或已分配序号的 segment 没有处理。flush 发现当前缓冲放不下时先发送旧缓冲，然后仍完整编码该大 segment。

实测先 send 1000 字节，再 set_mtu(100)，输出为 1024 字节；已发出后丢弃报文、缩小 MTU，再触发 RTO，重传仍为 1024 字节。

建议拒绝存在不兼容待发送/在途 segment 时的缩小操作，或要求 wait_send=0 后再修改。不能随意重新切分已分配 SN 的在途数据，这会影响序号与对端去重。

证据：`audit_core.rs` 中 decreasing_mtu 与 inflight_segments 两项。

### 12. P2：极端 RTT 在 clamp 之前发生中间算术溢出

位置：[kcp.rs:1090](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:1090)。

`rx_srtt + max(interval, rx_rttvar << 2)` 使用 u32 计算，之后才 clamp。构造 conv 正确、时间差 1,431,655,766ms 的 ACK，debug 因加法溢出 panic，release 回绕后错误地得到 30ms RTO，而不是饱和到 60000ms。该触发需要极端时间戳；不是声称所有普通 ACK 都能使连接崩溃。

建议中间乘加使用 u64，安全计算后 clamp 到 rx_minrto..60000，再转为 u32。工作区明确要求保留 Go kcp-go 对未命中 snd_buf 的 ACK 也采样 RTT 的行为；这不是本报告的 bug，不建议过滤这类 ACK。修复算术即可，不改变采样语义。

证据：`audit_core.rs` 中两项 old ACK 检查；debug 和 release 均已验证。

### 13. P2：FEC 构造允许总数 256，自动探测却要求小于 256

位置：[fec.rs:59](/Users/sean/Documents/kcptun-rs/kcp-rs/src/fec.rs:59)、[fec.rs:493](/Users/sean/Documents/kcptun-rs/kcp-rs/src/fec.rs:493)。

encoder/decoder 接受总数等于 256；auto-tune 条件却为 `<256`。合法边界配置无法被另一种初始配置的 decoder 自动适配。

实测 encoder=128+128，初始 decoder=3+2，连续四组每组丢一个数据包。匹配的参考 decoder 恢复 4 包，自动探测 decoder 恢复 0 包。表现为 FEC 恢复能力失效、需要 KCP 重传补偿，不能直接等同于永久应用数据丢失。

建议统一合法参数范围，对总数 255、256 和非法 257 增加边界验证；保持 FEC wire format 不变。

证据：`audit_fec.rs::accepted_256_shard_config_cannot_be_auto_tuned`。

## 次要健壮性与可观测性问题

这些项目不应与上面的常用路径问题混用严重度。

- **零长度末尾分片留下空的“幽灵消息”。** [kcp.rs:499](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:499)、[kcp.rs:537](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:537)、[kcp.rs:567](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:567)：按累积字节数判断消费完成，而不是消费到 frg=0；recv_bytes 快速路径也仅比较长度。构造 sn0/frg1/data=a 与 sn1/frg0/空数据，会先返回 a，再返回多余空消息。正常 send 不生成空末片段，主要是输入健壮性。建议按分片终止条件消费，单片快速路径检查 frg=0。已有 audit_core 复现。
- **FEC 公共 API 的 SIZE 字段可截断。** [fec.rs:103](/Users/sean/Documents/kcptun-rs/kcp-rs/src/fec.rs:103)：65534 字节 payload 加 2 字节 size 字段变成 65536，转 u16 得到 0，恢复包随后被拒绝。普通 UDP 负载本身低于此边界，不能描述为默认 UDP 热路径 bug。公共 API 应验证 payload 不超过 65533，并返回明确错误。已有 audit_fec 复现。
- **过短 encode buffer 可 panic。** [fec.rs:189](/Users/sean/Documents/kcptun-rs/kcp-rs/src/fec.rs:189)：3+2 配置连续输入三个空 buffer，RS payload 切片越界。调用者违反了注释中的布局前提，因此列为低优先级 API 防御；建议显式检查前提并返回错误，不称为已确认的远程漏洞。已有 audit_fec 复现。
- **SNMP 活跃连接 gauge 在 reset 后关闭会下溢。** [snmp.rs:292](/Users/sean/Documents/kcptun-rs/kcp-rs/src/snmp.rs:292)、[snmp.rs:346](/Users/sean/Documents/kcptun-rs/kcp-rs/src/snmp.rs:346)：session_opened → reset → session_closed 得到 u64::MAX。建议 reset 保留当前 gauge，或明确重新计数规则并保护减法。迟开统计也要有一致语义。已有 audit_fec 复现。
- **连接计数没有在 crate 内接入生命周期。** [snmp.rs:322](/Users/sean/Documents/kcptun-rs/kcp-rs/src/snmp.rs:322)：session_opened/session_closed 在 kcp-rs 内仅有自身测试调用，conn/listener 不调用。只使用本 crate 不会自动得到正确连接数；本次未检查外部调用者，不能断言整个 kcptun 产品的统计恒为 0。建议在创建/最终关闭的唯一位置接入，注意 owner/clones 与迟开统计，避免重复计数。
- **SNMP README 示例不匹配公开 API。** [README.md:323](/Users/sean/Documents/kcptun-rs/kcp-rs/README.md:323) 的 Display 示例用于 DEFAULT_SNMP，实际 Display 在内部 snapshot 类型上。建议改用可用的公开输出 API；这是静态接口检查，未单独运行该文档片段。

## 优化建议：先减少保留内存，再优化热路径

已有批量收发、segment pool、锁外 FEC 处理与事件驱动空闲等待值得保留。此次没有做性能基准，不承诺吞吐或延迟提升百分比；以下收益需测量验证。

| 顺序 | 建议与代码依据 | 收益与验证方式 |
|---|---|---|
| 1 | 修正 [PeerBuffers.spare](/Users/sean/Documents/kcptun-rs/kcp-rs/src/transport.rs:225)：pop_into/pop_batch 只 push，没有实际取出复用 | 改为归还全局池，或补齐真实复用。典型每个 buffer 容量 2048 时，64 个约 128KiB/peer；4096 个经历过该路径的 peer 约 512MiB。并非新建连接即占用，64 也是数量限制，较大 capacity 不会被 resize 缩小。比较 burst 后空闲 RSS 与分配次数 |
| 2 | 为 [SegmentPool](/Users/sean/Documents/kcptun-rs/kcp-rs/src/segment.rs:274) 增加保留字节预算和过大 buffer 淘汰规则；审视 [parse_ack clear](/Users/sean/Documents/kcptun-rs/kcp-rs/src/kcp.rs:635) | BytesMut::clear 清长度而不释放 capacity；当前 pool 只限对象数量。高窗口或大 MTU 峰值之后会保留内存。用同一 workload 比较峰值及恢复空闲后的 RSS、alloc/吞吐，避免过度释放破坏复用 |
| 3 | [FecEncoder](/Users/sean/Documents/kcptun-rs/kcp-rs/src/fec.rs:66) 缓冲懒分配，容量随实际数据报大小管理 | 10+3 当前初始化 payload cache 为 19500B/连接；4096 个启用该配置的连接仅此项约 76MiB，未含 codec/decoder。比较大量闲置 FEC 会话 RSS、首包延迟与持续吞吐 |
| 4 | FEC 分组改为固定 slot + presence bitmap，减少 heap/hash/sort | 分组尺寸已知，可避免部分逐包组织开销；排序只发生在恢复路径，完整数据组直接完成。必须保留完整 group id、重复过滤、已完成组记录、seqid 回绕与自动探测重建，避免混合不同组。测 10/3、边界 shard 数，以及不同丢包/乱序分布下 CPU、alloc、恢复率 |
| 5 | [每 IP 会话数检查](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:979) 维护计数，避免每次遍历 sessions/building | 新 peer admission 当前为 O(现存会话数)，此 cap 默认关闭。计数增减需覆盖 build 失败、timeout、publish、remove、reap。比较 1k/4k 会话下新会话耗时 |
| 6 | [IP limiter](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:668) 使用真正容量限制和固定清理预算 | >4096 时的 retain 只删除超过一秒的项；新鲜不同 IP 仍可继续增长，且每次新项都可能全表扫描。设硬上限与周期清理，比较高基数来源输入下单轮处理耗时与 map 大小；此次是复杂度判断，未做压力测试 |
| 7 | 复用 listener 每 peer burst 的内部 Vec，避免只复用外层容器 | [sharded.rs:890](/Users/sean/Documents/kcptun-rs/kcp-rs/src/sharded.rs:890) 的内部小批次仍分配。先 profile，视批次分布考虑 SmallVec 或 bucket 复用；测 alloc/packet 和 P99，不预设容器替换一定有收益 |
| 8 | 裸 KCP API 可提供可选发送队列字节上限与 try_send 背压 | 直接使用 KCP::send 可持续堆积 snd_queue；异步 facade 已有窗口相关限制。保持默认兼容性，明确拒绝时无部分入队，与问题 3 一并验证 |

进一步的小优化可研究 FEC decoder 的入队复制，以及 parity buffer 清零是否重复；应先测量和核对 RS 库的覆盖行为、不同 shard 长度的补零规则，再动代码。不能为了少一次 memset 破坏恢复结果。

修复和优化都应保持 24 字节 KCP 头、FEC 格式、序号回绕与已明确保留的 Go ACK 采样语义。不要直接通过更改默认窗口、启用忙轮询或重写运行时来掩盖问题。

## 验证记录与限制

环境：macOS；rustc/cargo 1.99.0-nightly；原工作区锁中的 tokio 1.52.4；本地 UDP、内存 transport 和可控 KCP 模拟时间。

| 检查 | 结果 | 实际范围 |
|---|---|---|
| 同步 lib 单元测试 | 56 通过 | 临时隔离 manifest，生产 src 不变 |
| 完整工作区默认 lib + integration | 编译失败 | 缺少 KcpConfig/KCP::apply，已在原复制工作区确认 |
| 完整工作区 async lib + integration | 144 项记录为通过 | 其中 3 项 raw TCP 测试在 macOS 条件分支提前返回；实际执行 141 项，不能称 raw TCP 验证成功 |
| 新增 core + FEC 复现 | 12 通过 | 断言现有错误行为；包含正常 message/stream 对照 |
| 新增 async 回归 | 6 失败、2 对照通过 | flush、shutdown、fallback、FEC 参数、接收时钟、大 MTU |
| 新增 listener 回归 | 2 失败 | building 取消与 pending 清理 |
| release RTT 定向检查 | 已复现 30ms 错误 RTO | 两个 ACK 用例；筛选命令还匹配一项旧 MTU 复现 |

原基线命令（在完整复制工作区）：

```sh
cargo test --offline --locked -p kcp-rs --lib --tests
cargo test --offline --locked -p kcp-rs --features async --lib --tests
```

可交付复现包：[kcp-rs-review-20261011](/Users/sean/Documents/kcptun-rs/bugs/kcp-rs-review-20261011/README.md)。包与生产 crate 分离；README 说明哪些断言记录错误现状、哪些应在修复后通过，并保留运行日志。默认构建失败的原始工具输出已在本次会话观察，交付包记录其摘要，不把摘要伪称为完整原始日志。

本次没有运行 Go 互通 E2E、Linux recvmmsg/sendmmsg/SO_REUSEPORT 或 raw TCP 的运行验证，也没有做性能 benchmark。Linux 专属路径及跨语言修复兼容性仍需后续针对性验证。此前审查时，GitNexus 的本地入口缺失，已注册索引又指向当时权限受限的原目录，因此调用图查询未成功；采用直接源码交叉检查，未修改或重建索引。本次仅同步审查资料，未重新查询调用图。

建议修复顺序：先 flush/shutdown 与 pending/building 生命周期，再修 stream send 的原子性和配置验证，随后处理 message/MTU/RTT/FEC 边界；正确性回归通过之后，再按上表逐项测量内存与热路径优化。
