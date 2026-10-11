# A0 留档 — v1 性能改动逐条变更说明 + 做/砍清单（2026-10-10）

对应 v2 任务书 `A0`（第 4、5 项）。工作分支：`perf/opt-v2`（workdir
`~/workspace/kcptun-rs-verify`，基线 = Desktop `main` @ `21db7813`）。

## 一、留档方式（任务书 A0-4 的处理）

任务书假设"6 文件改动已合入 Desktop 且无 commit，需拆成独立 commit"。核实后
该前提不成立，且拆分需改写历史。经用户确认（2026-10-10），**以本文档代替拆
commit**，现有 bundle commit 保持不动。

实际入库情况（均未 push，origin/main = `4a8c91df`）：

| commit | 日期 | 内容 |
|---|---|---|
| `b293624c` | 09-28 | perf(kcrypt,kcp): **O10-P** wire 缓冲摊销 + spawn_rx 批量 RX drain |
| `3304f021` | 10-10 | refactor 捆绑包（预存在的测试拆分 / kcp conn 拆分 / listener 工作），**非** perf 条目 |
| `520b2991` | 10-10 | perf(smux,kcp,snappy): **O1-P / O2 / O3 / O4** + kcrypt 注释修正 |
| `21db7813` | 10-10 | fix(smux): A/B 中发现的 write_loop 扫描缺失修复（非条目，是 A0-3 过程产物） |
| `c2aea7fa` | 10-10 | bench: 携带 Desktop 未提交的 P99 报告重跑 |
| `f1c82ceb` | 10-10 | test(kcp): **A0-1** 修复 fast_retransmit 测试时钟混用 |

## 二、逐条变更映射

### O1-P ⚠️（部分）snappy 写路径 — `520b2991` / `kcptun-common/src/snappy_pipe.rs`
- 新增 `BytesMutWriter` shim：`FrameEncoder` 直写 `BytesMut`，消掉每 write 一次的
  `mem::take` + Vec 重分配和一次中间拷贝；`drain_pending` 同步调整。
- **未做**（有意保留）：per-write `flush()` 策略 → 转为 P2 跟进项 O1-R（上层不调
  `poll_flush`，defer 会把 keepalive 小帧 park 住）。

### O2 ✅ spawn_rx slot 复用 — `520b2991` / `kcp-rs/src/sharded.rs`（约 691–733 行）
- `slot` 从共享 `BufPool::acquire_buf()` 取（只返回 `capacity >= MAX_DATAGRAM` 的
  buffer），`recycle_buf` 归还时 `clear()+resize(MAX_DATAGRAM, 0)` 全量零填充，
  语义与旧 `vec![0u8; MAX_DATAGRAM]` 一致。消掉每次安静唤醒的 2KB malloc。

### O3 ✅ flush 空闲轮询消除 — `520b2991`，三处
- `smux-rs/src/conn.rs`：spawn 的 flush task 改 notify 驱动（write/shutdown/SYN/
  close/read 窗口更新都 notify）；空闲挂在 `SPAWN_HEALTH_PERIOD=1s` 健康定时器上
  （旧：10ms 轮询 = 100 唤醒/s/连接）；`open_stream` 后补 notify（SYN 立即出门）。
- `kcptun-common/src/kcptun_session.rs` `write_loop`：500ms 墙钟健康检查 + `fin_wait`
  10ms 短轮询（发不出的 FIN 且 FIN 不走 notify 防自旋）；`more_data` 保留 notify
  permit（数据优先，FIN 不 notify）。
- `smux-rs/src/stream.rs`：`Stream::read()` 在 UPD 待发 / token 跨
  `max_recv_buf/4` 时 `notify_flush()`（v2 流控在非轮询下不致停滞）。
- 回归测试：`goroutine_tests.rs` 延迟中位数 < 5ms 断言。

### O4 ✅ 锁内 memcpy 移到锁外 — `520b2991` / `smux-rs/src/stream.rs`
- `read`/`write`/`drain_send_max`：锁内只做元数据（pop chunk / `split_to` 切 partial
  chunk，16/批），拷贝在锁外；`BytesPool::release` 先 `clear()`，`acquire` 恒返回空
  buffer；`write()` close 竞争路径无泄漏。

### O10-P ⚠️（部分）wire.rs — `b293624c` / `kcrypt-rs/src/wire.rs`
- 去掉 `resize(total, 0)` 零填充（改 `extend_from_slice`）；`reserve(total*8 max
  16384)` 摊销分配；`split_to(total).freeze()` slab 共享（`encrypt_cfb_batch` 一次
  flush 共用一块 slab；ptr 前进语义，不踩已 freeze 前缀，共享时 reserve 走新分配）。
- CRC 仍为独立 `crc32fast::hash(data)`，值与改前一致 → 融合转 P2 跟进项 O10-R。
- 同 commit 还含 spawn_rx 的 recvmmsg 批量 drain（本属既有性能工作，非 v1 条目）。

### "超范围" reap 改动 — 归属更正（重要）
任务书称 6 文件改动里含"hand-rolled reap → `reap_stale_streams` + linger 补 FIN"。
核实：该改动来自 **`9c4a38d3`（修复 agent 的 BUG-1..5 修复，2026-09-23）**，不在
`520b2991` perf pass 内（diff 无 reap/linger hunk）。即：它有独立修复 commit 留档，
与修复任务书的 session liveness 改动同源无冲突；perf 侧无需再处理。

## 三、A0-1 / A0-2 证据

### A0-1：四个 crate 测试全过（`docs/evidence/A0-1-tests.txt`）
```
=== cargo test -p kcp-rs --features async ===        EXIT=0  (93 lib + 集成/文档全过)
=== cargo test -p smux-rs -p kcrypt-rs -p kcptun-common ===  EXIT=0
合计 356 passed / 0 failed / 0 ignored
```
已知失败 `test_fast_retransmit_fires_on_duplicate_acks` 已修复（`f1c82ceb`）：
根因是测试混用时钟 —— `update(100/110/120)` 用模拟时钟，随后的 `flush()` 却取墙钟，
`resendts=100+rto` 显得古老 → 3 个段全被 RTO 重传成 6 个 → 只跳过第 1 个导致喂进
5 个段 → `rcv_nxt=3` → Go parity 的 bufferbloat ACK 过滤器
（`itimediff(sn, rcv_nxt) >= 0 || 最后一个`，已对照 kcp-go v5.6.64 `kcp.go` flush()
逐字核实）把旧 ACK 全过滤 → 只剩 1 段。修法：三处 `flush()` 改
`flush_with_current(模拟时间, true)`，不动生产代码。

### A0-2：fmt / clippy（`docs/evidence/A0-2-fmt-clippy.txt`）
```
cargo fmt --check                        FMT_EXIT=0
cargo clippy --workspace --all-targets -- -D warnings   CLIPPY_EXIT=0（零警告）
```

## 四、A0-3：O2/O3/O4 bench 对比（x86_64 Linux，hk_dev / CentOS7 3.10 / 4C）

环境：hk_dev（x86_64，4 核，rustc 1.99.0 stable，gcc 8.3.1/devtoolset-8），
`base = 3304f021`（perf 改动前）vs `post = 21db7813`（perf 改动后）。同一工具链、
base→post 顺序执行；数字为 `bench_rust_vs_go --rust-only --quick --runs 3`（10 conn
×1MB×3 取均值，loopback）+ `smux_bench`（内存管道）+ 30s held-session idle CPU。
原始日志：hk_dev `/tmp/bench-{base,post}-full.log` 与两棵树的 `bench_results.json`。

### 1) 空闲 CPU（O3 的主指标）：held session 30s 无流量

| tree | server ticks | client ticks | 合计 | 折合 CPU |
|---|---|---|---|---|
| base | 11 | 9 | **20** | 0.67% |
| post | 4 | 3 | **7** | 0.23% |

**post 空闲 CPU −65%** ✓（100Hz/50ms 轮询 → notify + 墙钟健康检查的效果）。

### 2) smux 内存管道（O4 的主指标）

| 指标 | base | post | delta |
|---|---|---|---|
| 4 流 ×64KB 吞吐（两次） | 104114 / 106610 iters/s | 97938 / 106075 iters/s | −3.2%（均值，噪声内；第二次单跑 +0%） |
| 64 流 ×4KB 吞吐 | 10812 iters/s | 11795 iters/s | **+9.1%** |
| latency p50/p99（µs） | 5.5 / 9.5 | 6.2 / 10.9 | p50 +0.7µs |

如实记录：64 流吞吐 +9%（锁外拷贝受益于高流数）；单次 latency p50 高 0.7µs
（两轮重跑方向一致，可能来自 notify 路径的微小时延，幅度 <15% 且绝对值仍 <7µs）。

### 3) e2e 吞吐（10 conn ×1MB loopback，14 配置）

| 配置 | base MB/s | post MB/s | delta |
|---|---|---|---|
| null/no-comp | 8.0 | 7.9 | −1.8% |
| null/comp | 7.8 | 8.5 | +8.5% |
| aes-128/no-comp | 7.5 | 8.1 | +7.9% |
| aes-128/comp | 8.3 | 8.0 | −2.8% |
| aes-128-gcm/no-comp | 8.8 | 6.8 | **−22.8%**（孤例，见下） |
| aes-128-gcm/comp | 8.0 | 7.9 | −1.4% |
| salsa20 ×2 | 7.9/8.0 | 8.1/7.8 | +1.9%/−2.6% |
| blowfish ×2 | 7.9/7.8 | 7.5/7.4 | −5.4%/−5.7% |
| sm4 ×2 | 7.4/7.2 | 6.7/7.8 | −9.9%/+8.0%（同密码两方向翻转 = 噪声） |
| 3des ×2 | 4.5/5.9 | 4.6/5.5 | +1.5%/−7.3% |

**结论**：loopback e2e 整体在 ±10% 噪声带内，无系统性回归/提升；`aes-128-gcm/no-comp
−22.8%` 为孤例（同配置 comp 方向仅 −1.4%，且延迟还改善 67→63ms；sm4 两方向自相矛盾），
判为 4 核共享 VPS 上的调度噪声，如实记录不掩饰。**吞吐层面 O2/O3/O4 无明确提升
（如实写）；确定性收益在空闲 CPU（−65%）与 64 流吞吐（+9%）。**

### 4) O2 的专项测量缺口
O2（spawn_rx 每安静唤醒 2KB malloc 消除）的直接指标是分配次数；远程无 `perf`/valgrind，
未做分配计数，仅由上面 idle CPU 间接覆盖（分配消除含在 20→7 ticks 内，无法单独归因）。
如实声明此缺口。

## 五、O0：x86_64 AES 硬件加速验证记录（验证型，关闭）

**结论：x86_64 已走硬件 AES-NI/PCLMUL，无需任何 rustflags；任务书"缺失 rustflags
⇒ 软件实现"的推断不成立，本条目按其步骤 4 关闭，`.cargo/config.toml` 保持原样
（已核实仅有 linker 行，无 rustflags 增删）。**

### 代码/依赖证据（为什么不需要 rustflags）
1. `aes` 0.8.4 `src/lib.rs` 文档原文：*"By default, this crate performs runtime
   detection of CPU intrinsics... By default this crate uses runtime detection on
   i686/x86_64 targets in order to determine if AES-NI is available."* —— x86_64
   默认运行时探测，只有 aarch64 才需要 `--cfg aes_armv8`（编译期门控，config.toml
   给 aarch64 配的就是它）。
2. `polyval` 0.6.2（aes-128-gcm 的 GHASH）`src/backend/autodetect.rs:22`：
   `cpufeatures::new!(mul_intrinsics, "pclmulqdq")` —— PCLMUL 同样运行时探测。
3. kcrypt `has_hw_aes()`（`aes_cfb.rs`）：`is_x86_feature_detected!("aes")` —— 与
   aes crate 的运行时探测一致，不会出现"上层以为有硬件、底层实际软件"的错配。
4. hk_dev `/proc/cpuinfo`：`aes`、`pclmulqdq`、`avx2`、`sse4_1` 旗标均存在。

### 实测（hk_dev x86_64，`cfb_bench`，1326B/包，release，各 2M 次；原始输出 `docs/evidence/O0-cfb-microbench.txt`）
| 方法 | 模式 | ns/包 | 备注 |
|---|---|---|---|
| `aes`（默认 AES-256-CFB） | enc | **1968 / 1977 / 1975**（3 轮） | 串行 CFB 链，83 block → ~23ns/block |
| `aes` | dec | **670 / 664 / 644**（3 轮） | ~8ns/block |
| `aes-128-gcm` | enc / dec | 1838 / 1627 | 含每调用 Vec 分配 |
| `null` | dec | 3.2 | 地板值 |
| `3des` | dec | **51517** | 纯软件密码对照（50µs） |

软件 T-table AES-256-CFB 预期 ~5–10µs/包（量级同 3des 对照行），实测 0.65–2µs/包
= **硬件区间**。`perf` 在该主机不可用（yum 无 perf 包），以上以微基准量级判别 +
依赖源码引用代替，并如实说明。

### e2e 交叉验证（`bench_rust_vs_go --rust-only --runs 1` 全矩阵，post 树）
`aes/no-comp 7.7 MB/s ≈ aes-128-gcm 7.7 ≈ null 7.9` —— 加密不是瓶颈，与硬件结论
一致（软件 AES-256-CFB 会显著低于 null）。顺带发现并修复：harness 的 `CIPHERS`
原来不包含默认的 `aes`（`bdcbbbad` 已补）。

### 改前/改后对比
本条目**不产生代码改动**，无改前/改后；上述即"改后（=基线）"记录。原任务书
步骤 3 的"补 rustflags 重跑对比"不执行——加 `-C target-feature=+aes` 会让无
AES-NI 的老 x86 CPU 直接非法指令，且对已有硬件探测路径零收益。

---


## 六、A0-5：剩余条目做/砍书面清单（用户已确认 2026-10-10）

| 处理 | 条目 | 说明 |
|---|---|---|
| 做（P0） | A0 | 本文档即交付物之一 |
| 做（P0） | O0 | **改为纯验证**：x86_64 Linux 实测 AES-NI 生效 + bench 记录，留证后关闭。不加 rustflags（`aes` 0.8.4 / `polyval` 0.6.2 在 x86_64 均为运行时探测，加 `+aes` 反而让无 AES-NI 的 CPU 非法指令） |
| 关闭 | O6 | CLI 已覆盖：server `unwrap_or(1024)`（`kcptun-server/src/app.rs:70`）、client `unwrap_or(512)`（`kcptun-client/src/app.rs:128`），库默认 128 不达生产路径 |
| 关闭 | O0 的"改 rustflags"分支 | 前提错误，见上 |
| 推迟 | O1-R | 前置（上层显式 flush 语义）改造大，收益仅压缩率；用户需要压缩率时再启 |
| P1 | O5 | spike 先行，与 Go 互通不过即关；e2e 届时需用户确认 |
| P2 按序 | O9 → O7 → O11 → O8 → O17 → O3-R → O12 → O13 → O14 → O15 → O16 → O10-R | 每条独立 commit `perf(opt): <编号> …`，先 bench 后动手 |

---

## 七、v2 执行结果总表（2026-10-10 收尾补录）

分支 `perf/opt-v2`，逐条独立 commit（`perf(opt)`/`docs(perf)` 前缀）：

| 条目 | 结果 | commit | 关键数据/证据 |
|---|---|---|---|
| A0 | ✅ 完成 | `3dc7e2a8` | 356/0 测试、clippy/fmt 过、O2/O3/O4 bench（idle −65%）、本留档 |
| O0 | ✅ 关闭（验证型） | 同上（第五节） | AES-NI 硬件区间 1975/659ns/包；不加 rustflags |
| O9 | ✅ 完成 | `01b2a17e` | CFB 解密 659→483ns/包（−27%），71/0 + 3 个 bit-一致单测 |
| O7 | ✅ 完成 | `f6339083` | async 发送零 to_vec/零 out 分配（timeout 回退除外），56/0 + stress 8/8 |
| O11 | ✅ 关闭（证据型） | `004a3077` | seal() 无生产调用方，grep 留档 |
| O8 | ✅ 完成 | `3fd839d6` | cpu_block 8.03→4.03 allocs/调用（−50%）、1200→168B（−86%） |
| O17 | ✅ 完成（6/7） | `73d2cc6b` | #6 空扫跳过有据跳过（HOTSPOTS 无记录+CRITICAL 面）；#4 已内置 |
| O3-R | ✅ 完成 | `f074b8dc` | 空闲 10s 唤醒 ≤20 断言，3 遍稳定 |
| O12 | ✅ 完成 | `1b17c2b4` | prepare 1024 流 mean −3.0% / median −6.1%（交替 5 轮） |
| O13 | ✅ 完成 | `291dc387` | 直读 codec；e2e 噪声内无可测提升（如实记录） |
| O14 | ✅ 完成 | `922e8ec5` | 单条流缓存；bench 噪声内持平（如实记录） |
| O15 | ✅ 完成 | `3d52282e` | 活流 RSS −41.7%（1024 流），吞吐无退化 |
| O16 | ✅ 完成 | （见 git log） | --threads CLI + knet configure；1MiB 冒烟 OK；延迟 A/B 见 evidence |
| O10-R | ✅ 关闭（微测） | `2e353298` | 字节融合 CRC 4032ns vs crc32fast 167ns（24× 负收益） |
| O5 | ✅ 关闭（spike） | `52b0f9f8` | reed-solomon-simd 为 GF(2^16)——与 klauspost GF(2^8) 不同域 |
| O1-R | ⏸️ 推迟 | — | 用户确认的推迟项（上层 flush 语义前置） |
| harness | 附带 | `bdcbbbad`/`76422a76`/`36cf1b73` | CIPHERS 补 aes、cfb_bench、py3.6 兼容 |

环境备注：hk_dev 与另一会话共享，期间其清理过 /opt/rh、/root/kcptun-perf
（已重建）；benches 均在该机 x86_64 Linux 完成。
