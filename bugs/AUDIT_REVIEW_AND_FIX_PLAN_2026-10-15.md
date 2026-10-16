# 外部审计逐项复核与修改方案

日期：2026-10-15。基线：`e532b3a3ee87f543ea29d91cefc7610ea73a712c`，加当前工作区内容。

输入：用户提供的《我把整个工作区……》审计文本。本文覆盖原文所有问题组，区分缺陷、优化机会、误报和证据不足项。**本次只生成复核文档，不实施业务代码修改。**

## 1. 审核口径与结论

原文找到了真实缺陷，但不能直接作为实施任务书：部分严重程度过高、性能数字解释不准确，若按原修法机械实施会引入回归。优先处理 SMUX 接收记账/回收、`cpu_block` panic、Linux mmsg 初始化契约、加密 MTU 边界及解析资源上限；把扫描和拷贝优化单独立项。

证据等级：

- **实测**：本次运行了针对该结论的独立验证。
- **源码确认**：已检查当前代码及相关路径，能给出失败条件；不等同于线上复现。
- **条件成立**：实现现象属实，但影响取决于平台、负载、配置或调用方式。
- **不成立/保留现状**：原文因果或建议不成立，或者与目标 Go 版本已有行为一致。
- **待验证**：没有足够证据认定收益、死锁或外部 API 使用情况。

原有 `bench/LATENCY_P99_REPORT.md`、`bench_results.json` 有用户未提交修改，本次保留。报告中的 raw KCP 数字确有 182341 vs 87000 req/s，但测试明确排除了加密、FEC、SMUX、Snappy，不能推出整个工作区没有显著性能瓶颈；其 RTT 也不含 write 接纳前的等待。未重跑这些历史基准。

### 必须纠正的关键结论

| 原文主张 | 复核结果 |
|---|---|
| FEC 在配置 MTU 外再加 8B | 错。`conn.rs:1374–1386` 已把 8B 从 KCP MTU 扣除；加密外层开销仍未统一扣除 |
| `mark_fins_sent` 每次成功写出都全扫 | 错。`session.rs:1158` 对空 ids 立即返回；非空才 O(N×K) |
| O12 的 230µs 是单次 prepare/遍历成本 | 不严谨。当前 bench 计时包含所有 stream 的 refill、write、分配以及 prepare；历史证据没有纯遍历计时 |
| FEC 调参失败会丢掉每一个 KCP 数据包 | 错。decoder 不恢复，但 endpoint 仍把原始 DATA 的 KCP 部分直接送入；日志本身也有误导 |
| FEC 每个 episode 只探测一次 | 不可采用。首次样本不足后必须允许后续样本重新触发，否则无法恢复 |
| 未知 FEC 标记回退裸 KCP 是策略矛盾 | 对目标 Go v5.6.64 不是缺陷；Go `kcpInput` 也有裸包 default 分支 |
| FEC 的 500ms 疑似错误 RTO | 对目标 Go v5.6.64 不是缺陷；Go 使用 `maxFECEncodeLatency = 500` |
| tcpraw 批量接收每个 payload clone 两次 | 所列函数只有一次 `payload.clone()`，另一次是 move |
| Snappy `replace(Vec::with_capacity(64KiB))` 即可复用 | 不成立。仍然每批新分配，而且编码后长度可超过 64KiB |
| sm4/twofish 两套阈值必须统一 | 不成立。CPU 池 offload 与临时并行线程 spawn 是不同成本模型 |
| 仓库内没调用的 pub API 应统一 deprecated | 证据不足。发布库的外部调用者不可从本仓库推出 |

## 2. 优先正确性问题与具体改法

### A01 — SMUX 接收记账与最终回收竞态（原 P0-1）

**结论：源码确认，优先级 P1。原文的最小修补不够完整。**

证据：`smux-rs/src/stream.rs:425–441` 在 recv 锁外读取/增加计数；`:478–489` 的读取扣减已在锁内；`:647–654` 回收在锁内清队列并 swap；`:989–1027` close 同样清队列。`session.rs:650–687,805–824` 先取 Arc，再解开 streams 锁，随后 consume/push；`:841–910` remove/reap 会终结回收。

可以构造的交错：

1. push 入队后解锁，尚未 fetch_add。
2. read 取出该队列，fetch_sub 从旧值扣减，产生暂时的 usize 回绕；或者 recycle 清队列并取走旧计数。
3. push 最后加 n；若流已被最终移除，则 n 再没有 session 回收者。

仅与普通 close 竞争时，之后若还有 reap，残留 n 可能被取回，因此“任何 swap 都永久泄漏”过强。更重要的是，**即使把 check/push/store 移入锁，已缓存的 Arc 仍可能在最终 recycle 之后入队**；原文没有覆盖这个生命周期窗口。

修改方案：

- 把容量校验、队列入队、字节计数更新放在同一 recv 临界区；用 `n > max - current` 或 checked_add 进行明确边界处理。
- 为最终移除增加受同一锁保护的“不再接受入站数据”状态，或提供原子完成“封闭入站 + 清理 + 返回全部 token”的内部方法。
- 区分 local half-close 与最终销毁：不能简单拒绝 `is_local_closed()` 的输入，否则会丢弃合法响应尾部。
- 封闭后 push 返回错误，由 session 现有错误分支退还本包 token；已入队数据只由一个回收路径返还。唤醒继续放锁外。
- 不必为这次修复把所有原子字段迁入结构体；先闭合不变量，再讨论结构调整。

验收：用 barrier/测试钩子固定 push↔read、push↔close、push↔reap、缓存 Arc↔remove 的交错；断言队列长度与计数一致、总 consumed = returned + buffered、无双返还、无永久窗口亏损；验证半关闭仍能收到尾部数据。新大测试模块放 sibling tests 文件。

### A02 — 加密 MTU 超出接收容量（原 P0-2）

**结论：源码确认的配置边界缺陷，P1；原文 FEC 计算和失效范围需更正。**

证据：`kcp-rs/src/conn.rs:53–79` 接受 50..=2048；`:1374–1386` FEC 已扣 8B；`transport.rs:29` 的 MAX_DATAGRAM 为 2048；`sharded.rs:877–892` 首包接收切片长 2048。`kcptun-common/src/kcp_transport.rs:267–305` 解密在收包后进行。CFB 外层 20B，当前 AES-GCM 为 12B nonce + 16B tag。

按当前配置语义，满包上限为 `config.mtu + crypto_overhead`，不是再加 FEC 8B。故 CFB 允许的容量边界应不超过 2028，GCM 不超过 2020，null 可到 2048；有无 FEC 不重复扣头。满尺寸加密包可能被截断/丢弃，Linux 批量路径也可能直接丢掉 MSG_TRUNC 包。小包仍可能成功，不能断言配置一设为 2048 就完全不能建立会话。

推荐最小修复：在 shared stack 组装配置和加密引擎时，用实际外层开销执行 checked_add 与容量校验，client/server/库构造路径共用校验。非法配置明确报错，不能仅警告后继续。原始 kcp-rs 仍允许无包装传输 2048。

另一方案是把 CLI MTU 定义为完整 wire 预算并像 Go `SetMtu` 一样扣所有头。这会改变既有分段行为，应单独决策、更新双语 README/CHANGELOG，不与边界拒绝修补混在一起。不优先给 PacketTransport 增加无默认实现的必需方法，这会破坏下游 trait 实现。

验收：null/none/CFB/GCM × FEC 开/关 × 边界±1；收集真正 wire 包长并确保 ≤ 接收槽；覆盖客户端和 listener 首包/批量路径。`none` 仍有 CFB 包装头，不能当 null。Go 互通仅在明确获准后运行。

### A03 — cpu_block panic 后结果等待不结束（原 P0-4）

**结论：源码确认；独立 unwind 构建验证见第 7 节。P1。**

`knet-rs/src/task/tokio.rs:95–101` worker 直接执行 job；`:130–150` await 侧保留 tx，job 持 tx clone。f panic 杀死该 worker 并 drop tx_job，但 tx 仍存活，结果 channel 不关闭。依赖 release panic=abort 不会修复发布库在 unwind profile 下的问题。生产加密/压缩部分调用有 200ms timeout，不能泛化为所有调用者永久停顿；pool 的 worker 减员仍然存在。

推荐：结果通道改为携带 `std::thread::Result<R>`，job 内 catch_unwind，await 侧收到 Err 后 resume_unwind；只在确认结果已消费后归还配对通道。可以局部使用 AssertUnwindSafe，但需说明共享状态在 panic 后的可用性，不应给全部调用者新增 UnwindSafe 约束。调用者取消后迟到结果不能回流到别的任务。

只 drop 本地 tx 虽能让 receiver 结束等待，但会破坏现有 channel 复用方式，且无法防止 worker 逐个死亡；不是完整修复。

验收：panic payload 在调用侧可捕获；多次 panic 后正常任务仍完成；取消/timeout 后无串结果；复用微基准不明显退化。GitNexus risk 为 **CRITICAL**，涉及加密、Snappy 写循环、文件读取及两个二进制入口；必须独立提交和回归验证。

### A04 — Linux mmsg 提前 set_len（原正确性 #3）

**结论：源码确认违反 unsafe 契约，P1；不应仅列为规范小项。**

`knet-rs/src/net/mmsg.rs:196–203,304–311,398–405` 在内核写入前 set_len(capacity)。容量中的新内存尚未初始化，不符合 Vec::set_len 的前提；不需要假设 panic 一定会读取这些字节才能认定问题。Vec<u8> 的析构本身也不等于读取每个元素，所以原文的 panic 解释应删去。

修改：保留 len=0，reserve 后用原始可写指针/`spare_capacity_mut()` 建立 iovec；只在 syscall 成功、长度验证完成且未 MSG_TRUNC 时 set_len(actual_len)。地址、iovec 和 msgs 缓冲在 syscall 期间必须稳定；错误、未填槽和截断槽均保持零长度。也可 resize 初始化作简易正确版本，但须测量额外清零成本。

验收：Linux 三个收包入口覆盖空批、WouldBlock、部分返回、截断、混合有效/无效槽、反复重用。macOS 编译/测试不覆盖这些函数。参见 [Rust Vec::set_len 安全约束](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.set_len)。

### A05 — tcpraw 阻塞发送与失败注册清理（原 P0-5）

**发送：条件成立，P1。** `knet-rs/src/net/tcpraw.rs:293–302` raw socket 未设 nonblocking，`:466–479` sendto flags=0；异步 send/batch 直接调用 raw_send。内核压力下会阻塞 runtime worker，但本次未做 Linux 堵塞复现，不能称已测得的停顿。

修法：优先按调用设置 MSG_DONTWAIT，避免把共享 fd 改 nonblocking 后误伤 capture 接收线程。try-send 返回 WouldBlock；async 层有界等待并重试，不能忙循环。**批量已经成功的前缀必须保留，只重试余下后缀；不能只在 sendto 加 flag 而仍以 Result<()> 丢失部分进度。** seq 只在实际成功发送后前进。

**注册泄漏：源码确认，P1。** `:786–812` 插入 channels/flows 后，capture_repair_state、install_server_rule、try_clone 都可能 `?` 退出；构造出 TcpRawConn 前没有 listener_reg Drop 清理。不同 peer 反复失败可累积失效注册；并非 capture 线程自行无限创建 channel。

修法：局部 rollback guard 覆盖所有失败出口，成功构造后移交所有权。删除前比较注册身份，避免清理同地址的新连接。检查 fd/TCP_REPAIR/iptables 资源的成功归属与失败释放；不要误删 listener 共用规则。

**分配：优化机会，P2。** `:322–335` checksum 每包临时 Vec，推荐直接对伪首部与 segment 累加一补和，伪首部 12B 是偶数，分段累加需正确处理奇数尾部及进位。`:563–583` 只有一次 clone；移除它前先确认公开 API 的 packet_bufs 输出契约，不能因仓库调用者不用就认定无用。

验收：Linux 权限环境下 EAGAIN/部分批次、序号单调和无丢重；repair/rule/clone 故障注入后注册数回基线；checksum 与旧实现对随机奇偶长度完全一致。

### A06 — pprof 串行服务与请求体无上限（原 P1-1）

**结论：源码确认，P1 健壮性。** `kpprof-rs/src/lib.rs:238–311` 接收后内联解析和处理；长 profile 会阻塞同一 listener 的后续处理。POST body 按 Content-Length 持续增长，每次 read 的 2s timeout 不限制总请求时长。

“PROFILE_BUSY 的 429 永不可达”只对单个正常运行的服务循环成立；该标志是全局的，公开 run_pprof 可在多地址运行，因此不宜全局断言不可达。

修改：拆分单连接 handler，通过 knet::spawn_task 并发处理，设活动连接上限、总请求截止时间、header/body 上限；只在需要 body 的路由读取它，超限早拒绝，超时终止。限流不能在一个慢连接占满许可时无限阻塞 accept；profile busy guard 需覆盖完成、失败和取消。CPU profile 的专用线程保持现状，不放回 cpu_block。记录真正 accept 错误，普通轮询 timeout 不刷日志。

验收：profile 执行期间 index/heap 可访问、第二 profile 快速 429；超大 body 早拒绝、慢速输入在总截止时间结束；stop 后任务有界退出。默认监听 loopback，远程可达风险取决于显式 bind 配置。

### A07 — FEC 未收敛时重复计算与日志（原 P1-2）

**结论：源码确认，P1/P2 视暴露面；不能按原建议只探测一次。**

`kcp-rs/src/fec.rs:338–465,584–632` 最多保留 258 样本；should_tune 为 true 时逐个有效 FEC DATA/PARITY 包重算周期、必要时 infer_dimensions，失败每次 warn。未知 flag 已在 `:550–560` 拒绝，不会直接参与推断。null 下伪造有效格式可放大计算；CRC 型封装不能等同于认证屏障。未测量攻击成本，不给出 CPU 占比。

当前 infer_dimensions 有 edge/候选/样本扫描，原文 `O(period²·samples)` 没有完整推导，删除该公式，以最坏分支基准代替。`endpoint.rs:1175–1200` 不依赖 decoder 成功即可输入原始 DATA，因此日志“still dropping every packet”应改为“尚未恢复 FEC 重建能力”。

修改：样本继续采集；以新增有效样本数、时间冷却或参数化的有限工作预算控制重新推断；有新证据后必须能重试和成功退出 should_tune。日志按状态变化和限频输出。先只做日志限频可独立降低风险，CPU 节流另做协议回归。

验收：错配→样本不足→最终收敛；丢包、乱序、重复、seq 回绕、1+1、不同 shard 比例；计量探测次数、日志上限；验证未收敛期原始 DATA 仍进入 KCP。

### A08 — Snappy 缓冲与分配（原 P1-5）

**解码资源：源码确认，P1。** `kcptun-common/src/snappy_frame.rs:29–88` 等待完整 24-bit 长度块，无独立字节预算；最大字段为 16,777,215，`chunk_len > 16_777_216` 分支永远不成立。单块可占近 16MiB；这不是无限长字段，但多会话可累积、public feed 也不限制单次输入。

**输出风险应一起纳入：** decompress_vec 在调用 SMUX 前分配解压输出；只限制编码输入还不够。应在分配前检查声明的解压长度。Snappy 数据块解压上限按 framing 规范为 65536B；skip 块的合法长度规则不同，不能对所有类型一刀切 64KiB。参见 [Snappy framing specification](https://github.com/google/snappy/blob/main/framing_format.txt)。

修改：按 chunk 类型验证长度，输入保留字节和输出批次均有预算；skippable 块可流式跳过；非法长度早报错，不能继续无界累计。`read_loop` 当前只 warn 后继续，应明确致命格式错误关闭会话或安全重置的策略，避免错误循环。`feed_into` 可复用外部输出，但要规定追加还是清空，旧 feed 保留包装。

**编码分配：现象成立、成本估算不成立，P2。** `kcptun_session.rs:758–768` mem::take 把 Vec 所有权交给 Bytes，不再持有该容量；这不是无意义丢弃，返回包仍需要内存。不能推出“约13次 realloc”，FrameEncoder 是分块写入而非逐字节翻倍。新建 64KiB Vec 只能预分配，不能称复用；不可压缩块连同头部可能超过此值。

验收：跨 feed 的部分头/部分块、合法 stream identifier、压缩/不压缩/skip 块、超大声明、非法解压长度；记录每批 alloc 次数与峰值保留字节。真正复用需保证下游 write 完成后才能归还输出所有权，并处理 offload timeout 的迟到 job，不能共享仍在使用的 Vec。

## 3. 性能项的核实和实施边界

### B01 — 会话写循环多次扫描（原 P0-3、P2 收尾 break）

**成立的是多次遍历，尚未证明它是主要瓶颈；优先级 P2。**

`kcptun-common/src/kcptun_session.rs:839–857,938–955` 分别是 pending 的 any、prepare、reap、收尾扫描。any 最好 O(1)，最坏 O(N)；prepare 使用共享 Arc snapshot，`session.rs:1024` 不持 streams 锁跑完整热循环，原文“3 与 2争同一把锁”过度简化。`mark_fins_sent:1157–1169` 只有非空 ids 时 O(N×K)。

`docs/evidence/O12-prepare-bench.txt:5–14` 确有 mean 230266 ns/op；但 `smux-rs/examples/prepare_bench.rs:57` 起的计时包含 refill 全扫和写入，且使用 SMUX v1、256KiB prepare budget。生产写循环 budget 为 64KiB，下载空发送队列场景更不能直接套用。`read_loop:485` 每读块 notify 属实，但 Notify 合并 permit，不等于同次数实际执行；1600 wake/s 缺少吞吐、读块长度和合并率实测。

推荐顺序：

1. 非空 FIN ids 用 streams.get(id) 标记，避免全 snapshot 扫描；保持未知 id 忽略及写成功后才标记。
2. common 与 `smux-rs/src/conn.rs:533–549` 的收尾扫描，两个布尔条件都 true 时 break；不是无条件提早结束。
3. 分离 prepare、refill、reap、锁等待计时，覆盖 1/64/1024 流及空闲/下载/双向负载。
4. 若确有瓶颈，再从 snapshot 收集 reap 候选，实际删除时锁住 map 重新检查资格和实例身份；保持 linger、尾部数据、FIN 写成功顺序和 token 回收。
5. 不直接引入会话级 pending/FIN 原子计数。Stream 公开写入、drain、close、remove 等路径均须正确维护边沿；丢一次更新可能导致任务永久休眠，这比额外扫描更危险。

验收：v1/v2 窗口为零、KCP 堵塞、未发 SYN、FIN 延迟、写失败、并发 open/remove；除 CPU/吞吐外报告 P99/P999、重传、RSS 和分配次数。

### B02 — 默认 profiling allocator（原 P1-3）

**事实成立，收益未量化，P2。** 两个 main 的 pprof 默认 feature 使用 ProfilingAllocator；`kpprof-rs/src/heap.rs:60,129–147` 默认采样率 524288，rate 非零时 load/TLS/共享 fetch_add 在热路径。只有测试 reset 写 SAMPLE_RATE。编译时关闭 pprof 是现有选择，因此应写“缺少生产运行时关闭接口”，不能泛称完全没有开关。

rate=0 时仍有原子 load 和分支；`heap.rs:214–216` 写“single atomic add”与实现不符，“零成本”也不准确。不能凭估计承诺低个位数百分比。

修改：增加明确的启动配置入口，决定是否在未开启 pprof HTTP 时关闭采样；记录启动采样时刻。不要简单在访问 heap 端点时临时开启，否则无法获得此前分配历史。动态开关若支持，需定义计数/样本是否清零及跨阶段 alloc/free 统计含义。

验收：采样关闭/开启 × 单线程/多线程 × 不同对象尺寸；校验关闭后样本不增长，开启后输出有效，报告争用与尾延迟。

### B03 — 解密 memmove（原 P1-4）

**源码确认的优化机会，P2，收益未测。** `kcp_transport.rs:288–291` CFB 移动 payload；`kcrypt-rs/src/crypt/aes_gcm.rs:130` GCM 同类移动。CFB helper 已返回 body 切片，不必为了它再造一套重复 API。

真正的约束是 PacketTransport recv 返回长度且上层按 buf[..n] 消费；只给 kcrypt 增 offset 并不能贯通移除拷贝。需选定内部 packet view/range，让批量 compact、FEC、非 FEC、错误包处理均使用有效范围；旧公开 API 保持前移契约。先测解密+接收整条路径，CFB encrypt 微基准本身不能证明收益。

### B04 — 其它分配与复杂度（原 P2）

| 条目 | 判定与修改要求 | 验收 |
|---|---|---|
| `wire.rs:580–582` reserve | 真实容量预留错误。clear 后 len=0，reserve 的参数相对 len，不相对 capacity；应 reserve(packets.len())。不是越界，push 仍保证正确 | cap=8、目标9/12/16 时旧表达式预留不足；比对批量输出顺序与包数 |
| `transport.rs:280–297` 持锁 recycle | 锁内调用属实；`sharded.rs:115–138` 实际是 bounded channel 的 try_send/try_recv，未发现原文所暗示的反向互锁证据。属缩短临界区候选，不是已证实死锁 | 锁等待基准；移出回收不能为每包又引入独立分配 |
| `fec.rs:323–333,241–245,722–725` 分片分配 | Box 分片、引用 Vec 及恢复 clone 属实；第二次 to_vec 在 endpoint:1182/1190，不在相同 decoder 行 | 区分无丢包/恢复路径；先考虑移动恢复 Vec 后按 range 使用，池必须有字节预算，SmallVec 不要无条件内联256项 |
| `session.rs:320–327` snapshot 重建 | O(N) 分配/Arc clone 属实，但普通 stream close 并不必然重建；open/accept/remove/reap/session.close 才相关。“每连接3–4次”未证实 | churn 基准统计实际次数，再选批量变更/延迟重建；不要直接换复杂容器 |
| `aes_gcm.rs:63–66` seal | seal_into 后 to_vec 确有额外复制，但生产 CryptoBuf 使用 seal_into | 作为公开 convenience API 的优化，低优先，不因内仓少用就删除 |
| `kcp.rs:989–992` copy_to_bytes/reserve | 输出所有权转移可能导致补充分配；发生频率取决于 flush 产出包数和 BytesMut 余量，不是恒定每周期一次 | allocator 计数和不同 flush 大小实测；不能复用已交给 transport 的存储 |

## 4. 原“正确性/健壮性”九项逐条判定

| # | 结论 | 具体修改与验证 |
|---|---|---|
| 1 scavenger 锁内 close | `kcptun-client/src/app.rs:344–372` 属实；但引用的 client 注释讨论的是 conns 池锁，不能直接证明 tracked_sessions 的重入死锁。P2 防御性改进 | 锁内只决定并移出待关闭 Arc，锁外 close 和 Drop；保持 autoexpire 退休而非强杀语义。跑 session_retirement_test，先重建 release binaries |
| 2 IPv6 DSCP | 真实实现错误，P1/P2 配置相关。client socket:63–74、server socket:61–68 用 set_tos，socket2 0.5.10 socket.rs:1608 只调用 IPPROTO_IP/IP_TOS | IPv4 set_tos、IPv6 set_tclass_v6；平台 cfg 覆盖。Go v5.6.64 IPv6 确实不左移，按当前 Go 兼容目标保留该值语义并文档说明；若改成标准 DSCP<<2 须单独说明兼容差异。测 getsockopt/抓包，值0/46/63 |
| 3 mmsg 初始化 | 真实，提升优先级 | 见 A04，不能仅补 SAFETY 注释 |
| 4 unsafe Send/Sync | `tcpraw.rs:425–426` 确无说明，字段看起来可自动推导，但未做 Linux 移除编译 | 独立 Linux compile probe，证明自动 trait 后移除多余 impl；若保留必须写真实同步不变量；不得仅以 macOS build 验收 |
| 5 TCP 错误丢失 | `knet-rs/src/net/tokio.rs:585–604` 真实，但仅 hostname/fallback 分支；前面的已解析 SocketAddr 分支保留真实错误 | 先区分 TCP 地址与 Unix path；保留 TCP error kind/timeout/source，Unix 错误附上下文。测 IP、hostname refused、DNS失败、timeout、Unix成功/失败 |
| 6 AES 非法 key 长度 | `aes_cfb.rs:97–130` 真实 API 误用风险，超32还截断；生产 PBKDF2 固定32，非默认链路漏洞 | 新增 try_new/检查入口；旧 new 的兼容策略明确后再弃用，不直接改返回类型。测0/15/16/17/24/31/32/33及原合法向量 |
| 7 非 FEC 标记回退裸 KCP | 原缺陷判定不成立 | Go v5.6.64 sess.go:975–1065 同样按 typeData/typeParity 分支，其余 default 裸 KCP。保留原语义；非法 KCP 由状态机校验。Go另有 OOB 分支，不能因此假装本项目实现了OOB |
| 8 FEC 500ms | 原缺陷判定不成立 | Go sess.go:88 定义 maxFECEncodeLatency=500，:662 传给 encode。Rust endpoint:549/778 与其一致。可抽命名常量，不能替换成动态 rx_rto |
| 9 Frame.length 截断 | `smux-rs/src/frame.rs:86–94,130–136` 真实公开 API 风险，P2；字段公开还可与 data.len 不一致 | 增加 checked 构造/编码，对 payload≤65535、length一致做校验；旧签名迁移另行规划。测65535/65536以及人工篡改 length；内部 framing 路径保持原样 |

Go 对照均为本机缓存 `github.com/xtaci/kcp-go/v5@v5.6.64` 源码；可审阅对应 [sess.go](https://github.com/xtaci/kcp-go/blob/v5.6.64/sess.go)。这些是源码对照，不是本次运行 Go↔Rust e2e 的结果。

## 5. 原“规范与命名”九项逐条判定

1. **KCP/Kcp 命名**：风格选择，不是缺陷。保留 Go-mirror `KCP` 并解释即可；不为统一大小写增加新的公开别名和迁移成本。
2. **unsafe 注释**：实际存在缺失，规范修订合理；原文按 unsafe/SAFETY 字符串次数得出的比例不是逐块审查证据，本次不背书那些计数。先审 mmsg 初始化、fd 所有权、SIMD 对齐/长度/目标特性，再补说明；不能用注释掩盖不安全实现。旧 CODE_AUDIT_AND_STANDARDS.md 的“全部解决”应限定历史版本或修正。
3. **客户端 --threads 双解析**：`main.rs:33–49` 与 `app.rs:385` 属实。clap 后续仍会校验，不能称非法值被最终接受；问题是 runtime 初始化前发生副作用、对 `--` 和重复项处理不一致。改为 main 调用已有 parse_go_compatible，configure_worker_threads 后把 Cli 传入 async_main。保留 Go -ds/-ps 兼容。测试等号/分离形式、0、非法值、重复项、`--`、help/version，确保不提前建 runtime。
4. **文档漂移**：六个子项均有依据，但分类有区别：
   - `knet-rs/src/lib.rs:73–74`、`kcptun-common/src/pipe.rs:13` 应说明 closewait=0 等两向 EOF，非单向完成立即返回。
   - `kcp-rs/src/listener.rs:20–24` 的 cpu_block 描述过期；同时复核关闭/取消的真实行为，不能只替换线程名字便宣称 accept 一定立即取消。
   - `kcp-rs/AGENTS.md:20` 当前列 0xf3，但 Rust 没有该类型；Go v5.6.64 有 OOB，文档应写“本实现支持 f1/f2；不宣称 OOB 支持”。
   - `kcptun-server/src/cli.rs:212` 帮助应与 pprof_addr 默认127.0.0.1:6060一致。
   - 根 AGENTS 的 mimalloc 描述是底层分配器的简写，补充默认 pprof wrapper，比判为完全错误更准确。
   - `knet-rs/src/net/mmsg.rs:1–14` 宣称 macOS 支持与 Linux cfg 冲突，改成 Linux-only。
5. **公共死代码**：未见内仓生产调用≠没用；`encrypt_batch_into` 与 seal 是合理便利入口。没有替代方案和外部兼容证据不统一 deprecated。recv_buf_capacity 恒0应解释当前语义，必要时提供替代指标；不要删返回值或改 signature。
6. **加密阈值**：`wire.rs:419–431` CPU池门槛为4包/4096，`:528–537` 临时并行线程为8包/8192（而非原文所称更低）；两套成本不同，无矛盾。补文档或基准，不机械统一。
7. **localaddr 范围**：`kcptun-client/src/app.rs:35–42` 的确只取 next。最小修法是多地址明确拒绝或警告；推荐拒绝但需注明是可见行为变化。不要趁机实现多 listener；测试单地址、范围、IPv6、Unix path，避免把不支持范围误落入 Unix bind。
8. **小项**：`_real` 改为能表达保活目的的名字属于可选维护；`listener.rs:98–104` 空 CAS body 可简化但原逻辑仍设置 closed，不是“close无效果”；SNMP 顶部 Acquire 描述的是 AtomicU64 snapshot，`is_enabled` 是独立 AtomicBool，不能据此认定内存序矛盾，现 snapshot 确实用 Acquire。
9. **Go-ism**：同意保留。审计对应性比局部 Rust 风格统一更重要。

## 6. 建议实施批次与验收门槛

原文把“token竞态、池panic、FEC调参、MTU、性能优化、unsafe审计”都归为低风险 P0 不合理。本文 P1 表示需尽快修复的可靠性问题，P2 表示常规改进；尚无充分依据把全部项目定义成紧急发布阻断。

| 批次 | 范围 | 必须保持 | 验收 |
|---|---|---|---|
| 1A | A01 接收记账与最终封闭 | half-close、token恰返一次、FIN顺序 | 确定性交错测试 + smux测试 + session压力 |
| 1B | A03 panic隔离 | caller panic传播、worker存活、取消隔离 | unwind测试 + offload分配基准 + 加密/压缩回归 |
| 1C | A04 mmsg | 有效前缀、截断丢弃、已初始化长度 | Linux三入口单测/集成测试 |
| 2A | A02 MTU校验 | 不重复扣FEC、不改wire布局 | 边界矩阵 + 最大包长 + 获准后的e2e |
| 2B | A05 raw发送/注册 | 部分批次后缀、seq/顺序、失败清理 | Linux故障注入、权限环境集成测试 |
| 2C | A06/A08资源上限 | 正常profile/合法Snappy兼容 | 并发/慢请求/超限/分块测试 |
| 3 | A07计算限频、B01–B04优化 | 收敛性、无丢唤醒、无RSS换CPU隐患 | 有针对性的microbench + workload对照 |
| 4 | DSCP/CLI/API检查/文档 | 发布兼容、Go CLI解析 | 参数/地址矩阵、文档同步、平台编译 |

每个实现批次都先重新运行目标符号 impact，读最近 AGENTS；不要把本文的旧图结果当作未来改动的最新风险分析。业务代码用 knet facade。发布库新增 checked API 可增不删；更改必需 trait 方法、构造函数返回类型、公开字段类型均需明确破坏性版本策略。

建议命令（**这是后续实施验收计划，不是本次已通过的清单**）：

```sh
cargo test -p smux-rs --all-features
cargo test -p knet-rs
cargo test -p kcp-rs --features async
cargo test -p kcptun-common --all-features
cargo test -p kcrypt-rs
cargo test -p kpprof-rs
cargo test --workspace
make clippy
cargo fmt --all -- --check
cargo build --release -p kcptun-client -p kcptun-server
cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture
make stress
```

只对实际涉及的 crate/路径先跑定向测试，避免每个小文档修正都重跑全部。flush/lock/session 改动按项目要求跑 stress。测试若启动二进制必须先重建 release，避免命中旧文件。全 workspace 基线失败单列，不顺手扩大范围。`make clippy` 已自带 -D warnings，无需写不存在的 `make clippy -D warnings`。

`make e2e`/`test_e2e.sh` 仍需用户单独明确授权；此次审核不运行。实施后提交前运行 detect-changes，partial/truncated 不是通过。架构、配置和API改动同步最近 AGENTS、README/README.zh、CHANGELOG及相关bench文档；纯内部局部修补可明确“no AGENTS sync needed”。

## 7. 本次证据与限制

- 读取原附件，核对上列当前源码、O12 bench/harness、当前延迟报告、socket2 0.5.10 实现及 Go v5.6.64 sess.go。
- GitNexus 初始运行失败：PATH 中旧 Node 不支持 diagnostics_channel.tracingChannel；改用现有 `/opt/homebrew/bin/node` 后成功，无安装。旧图已落后于HEAD，执行 `analyze --index-only` 刷新。
- 图索引：7901 nodes、19745 edges、540 flows；索引提示流程遍历有截断和未覆盖入口，因此图中“无调用者”不能作为未使用证据。源码补查发现 Arc 方法调用等有漏边。
- 运行 query/context，以及四个 upstream impact：push_data_bytes=LOW（5），cpu_block=CRITICAL（28），process_inbound_batch=HIGH（4），raw_send=LOW（3）。括号为图 reported impactedCount；不是缺陷严重程度，更不是完整生产调用者计数。
- 图中 cpu_block 的直接调用包含 encrypt_with、write_loop、read_to_string；process_inbound_batch 包含 feed_raw_batch、spawn_input_loop。push_data_bytes 的图没有列出 session 的所有真实调用，已以 `session.rs:655/676` 补证。
- 独立临时工程验证位置：`/tmp/kcptun-audit-proof.v8Iog3/`，通过 path dependency 使用当前 knet；运行 `cargo run --offline --manifest-path /tmp/kcptun-audit-proof.v8Iog3/Cargo.toml`，退出码0。该临时工程独立解析缓存依赖，未采用工作区Cargo.lock，因此是针对当前实现的定向复现，不替代锁定依赖的回归测试。
- 实测输出：`reserve confirmed: before=8, required=9, after=8`；worker出现注入panic后，cpu_block在250ms内没有返回/传播panic，由外层timeout结束等待；随后正常job返回42，说明存活worker仍工作。永久等待的结论结合tx仍存活的源码推导，不由250ms观测单独推出。此验证不需要消耗全部worker。
- `git diff --check` 通过；新增文档另做尾随空白检查。最终状态仅增加本文，原有两个benchmark文件的修改保留。
- 未对 SMUX 竞态做生产负载复现；结论来自锁、生命周期与计数交错推导。未运行 Linux raw/mmsg 实验，未测新的性能收益，未运行 workspace/stress/e2e；不能将建议测试写成“已验证通过”。
- 本次只新增本审核文档，不改运行时语义/公共API，no AGENTS sync needed；没有提交。
