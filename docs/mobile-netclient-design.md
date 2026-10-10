# netclient —— kcp-rs 移动端通用网络客户端设计方案

> 状态: 设计稿 v1 (2026-09-29)
> 目标读者: kcptun-rs 维护者 / Unity & Godot 客户端开发
> 前置事实核对基于当前代码: knet-rs `block_on_local`/`block_on_multi_thread`(`knet-rs/src/task/tokio.rs:173,192`)、`KcpStream::with_transport`(`kcp-rs/src/conn.rs:200`)、`KcpStreamBuilder` 全套参数(`conn.rs:1055-1119`)、`CryptoTransport::new/with_transport`(`kcptun-common/src/kcp_transport.rs:90,95`)、`KcptunSession::connect`(`kcptun-common/src/kcptun_session.rs:111`)、`SnappyPipe::new`(`snappy_pipe.rs:51`)、`SmuxConn::connect`+`open_stream`(`smux-rs/src/conn.rs:119,218`)、根 `Cargo.toml` release profile `panic="abort"`(`Cargo.toml:11-16`)。

---

## 1. 目标与范围

**一句话**: 用 Rust 把 kcp-rs/knet 已有的能力封装成一个 **跨平台、跨引擎的通用网络客户端库**，以 C ABI 动态/静态库交付，Unity 与 Godot 都能直接用；拨号入口是统一 URL：

```
tcp://host:port      → knet::TcpStream          (可靠有序字节流)
kcp://host:port      → kcp_rs::KcpStream        (可靠有序字节流, UDP 底座, 可选加密/FEC)
kcptun://host:port   → kcptun-compat 全栈会话   (CryptoTransport + KCP + Snappy + SMUX 多路复用,
                                                 与 Go kcptun 服务端线兼容, 供直连现有 kcptun 服务器)
```

**范围内**:
- iOS (真机+模拟器) / Android (arm64-v8a, armeabi-v7a, x86_64) / Win / macOS / Linux（后三者保证编辑器 Play Mode 可用）
- Unity (C# P/Invoke) 与 Godot 4 (GDExtension 为主, C# P/Invoke 为备)
- 事件驱动的连接生命周期（连接/数据/关闭/错误）、拉取式收发、拨号超时、加密、可选 FEC、SNMP 式统计
- 客户端角色 only。服务端监听不在本库范围（kcp-rs `KcpListener` 已覆盖）。

**范围外（明确不做）**:
- 不可靠/乱序数据通道（kcp-go v5 线协议没有这个概念，破坏线兼容）
- TLS/WebSocket 封装
- 引擎内序列化/协议层（留给业务）

---

## 2. 总体架构

```
┌─────────────────────────── 宿主引擎 (主线程) ───────────────────────────┐
│  Unity (C# NetClient wrapper)      Godot (GDExtension / C# P/Invoke)   │
│        │  nc_dial / nc_send / nc_recv / nc_poll                          │
└────────┼─────────────────────────────────────────────────────────────────┘
         │ C ABI (稳定, 版本化, catch_unwind 包裹)
┌────────▼─────────────────────────────────────────────────────────────────┐
│  netclient-ffi  (cdylib + staticlib)  ← Unity 直接用; Godot C# 也可用     │
├──────────────────────────────────────────────────────────────────────────┤
│  netclient-core (纯 Rust lib, 可单测, 不含任何 FFI)                       │
│    Client ── command mpsc ──► RuntimeActor (专职线程)                     │
│      │                             │  拨号: TcpStream / KcpStream /       │
│      │                             │        KcptunSession                │
│      │  ConnHandle{Tcp|Kcp|Mux}    │  读任务: spawn_task(read→ring)      │
│      │  Registry: slab+generation  │  写任务: outbound ring → write_all  │
│      └── EventQueue (事件环, 给宿主 pump) ◄──┘                          │
├──────────────────────────────────────────────────────────────────────────┤
│  复用层: knet-rs (I/O facade) · kcp-rs(async) · kcrypt-rs · smux-rs ·     │
│          kcptun-common (CryptoTransport / KcptunSession / SnappyPipe)    │
└──────────────────────────────────────────────────────────────────────────┘
```

三条设计原则:

1. **netclient-core 不碰 FFI**。所有连接/拨号/事件逻辑是普通 Rust，`cargo test` 直接覆盖；FFI 层只是薄壳（句柄翻译 + catch_unwind）。
2. **全库遵守仓库规则: 业务代码只用 `knet::*`，不直接用 tokio**。runtime 的搭建走 `knet::block_on_local`（current-thread）/`block_on_multi_thread` 现成入口，在**库自建的专职线程**上驱动，不与宿主引擎线程纠缠。
3. **kcp-rs / kcptun-common 保持只读复用**。本方案只新增 crate，不改线协议，不动已发布 crate 的 pub API（个别只读 getter 的增量见 §12）。

---

## 3. 工程布局与硬约束

### 3.1 新 workspace（关键: 必须独立于主 workspace）

主 workspace release profile 是 `panic = "abort"`（`Cargo.toml:11-16`）。**FFI 库绝不能 abort**: 游戏进程里一个 Rust panic 直接杀死整个 App 是不可接受的。cargo 的 profile 只能在 workspace 根定义，所以新建独立 workspace:

```
netclient/                     ← 独立 workspace 根 (自带 Cargo.toml + [profile])
├── Cargo.toml                 # [workspace] members = ["core","ffi","godot"]
│                              # [profile.release] opt-level=3, lto=true,
│                              #   panic="unwind"  ← 与主 workspace 的差异点
│                              # [profile.release-mobile] inherits=release, opt-level="s"?
├── core/                      # netclient-core        (lib)
├── ffi/                       # netclient-ffi         (crate-type=["cdylib","staticlib"])
└── godot/                     # netclient-godot       (GDExtension, M5 可选)
```

- 对 kcp-rs 等用 **path 依赖**（`../../kcp-rs` 等），跨 workspace 的 path dep 完全合法；这些依赖编译进本 cdylib 时按本 workspace 的 `panic="unwind"` 编译，正好解决 abort 问题。
- 主 workspace 的 `Cargo.toml` 无需任何改动（可在 members 排除列表说明一下即可，实际上不同根自动互不干扰）。
- FFI 层每个 `nc_*` 入口再包一层 `std::panic::catch_unwind(AssertUnwindSafe(...))` 双保险，panic 时返回 `NC_EPANIC` 并把 `panic hook` 的输出路由到日志回调。

### 3.2 crate-type 矩阵

| 平台 | 产物 | crate-type | 加载方式 |
|---|---|---|---|
| Android | `libnetclient.so` (每 ABI 一个) | cdylib | `DllImport("netclient")`（IL2CPP 自动 loadLibrary） |
| iOS 真机 | `libnetclient.a` (+xcframework) | **staticlib** | `DllImport("__Internal")`（iOS 禁三方动态库） |
| iOS 模拟器 | `libnetclient.a` (arm64-sim/x64) | staticlib | 同上 |
| Windows | `netclient.dll` | cdylib | `DllImport("netclient")` |
| macOS | `libnetclient.dylib` | cdylib | 同上 |
| Linux | `libnetclient.so` | cdylib | 同上 |

一个 crate 同时声明 `crate-type = ["cdylib", "staticlib"]` 即可，构建脚本按 target 选择。

### 3.3 Cargo 依赖（全部走 knet facade）

```toml
# core
kcp-rs        = { path = "../../kcp-rs", features = ["async"] }
smux-rs       = { path = "../../smux-rs", features = ["tokio"] }
kcrypt-rs     = { path = "../../kcrypt-rs" }
kcptun-common = { path = "../../kcptun-common", features = ["tokio"] }
mimalloc      = { version = "*", default-features = false }   # 与二进制一致
```

knet 已被上述 crate 传递引入，core 直接 `use knet::...`。

---

## 4. 运行时与线程模型

### 4.1 线程拓扑

```
宿主线程 (Unity main / Godot main)          netclient-io 线程 (每 Client 实例 1 个)
┌────────────────────────────┐             ┌─────────────────────────────────────┐
│ nc_dial/nc_send/nc_recv:   │  command    │ std::thread → knet::block_on_local(  │
│  仅做: 句柄查表 + 入 mpsc   │ ──mpsc──►   │   run_actor(ctx))  ← current-thread │
│ nc_poll: 事件环出队 (无锁读) │             │   runtime, 全部 async 任务在这条线程 │
└────────────────────────────┘             │   + crypto/snappy 经 cpu_block 卸载 │
                                           └─────────────────────────────────────┘
```

- **默认 current-thread runtime**（`knet::block_on_local`）: 移动端省电、无跨线程调度抖动；游戏客户端连接数个位数，单线程 I/O 绰绰有余。重负载预留 `io_mode = multithread`（`knet::block_on_multi_thread`），配置项切换。
- CPU 密集（AES-GCM/soft-AES、Snappy）继续走 `knet::cpu_block` 卸载——kcptun-common 内部已经这么做了，FFI 库不要阻塞 io 线程的规则与 `bugs/BUGREPORT_TCPRAW_ACCEPT_STARVES_CPU_POOL.md` 的教训一致。
- **每个 `nc_client_new` 启动 1 条专职线程；`nc_client_free` 通过关闭信号 join 回收**。多 Client = 多线程，文档写明。

### 4.2 为什么是"命令队列 + 事件环"，而不是回调直调

- Unity IL2CPP 下从原生线程回调 C# delegate 需要 il2cpp 线程 attach，且 Unity 主线程 API 不可跨线程调用——回调式 API 是事故高发区。
- Godot 同理：只有主线程能改场景树。
- 因此 **对宿主只暴露 pump（拉取）语义**；可选回调作为第二通道（§7.6），文档标注"回调线程≠主线程"。

### 4.3 数据面

**发送（宿主 → 网络）**
1. `nc_send(client, conn, ptr, len)` 在宿主线程完成: 校验句柄 → **立即 memcpy 进该 conn 的 outbound ring**（cap 可配，默认 512 KiB）→ 返回 `len`（已接收的字节数）。
2. ring 满返回 `NC_EWOULDBLOCK`（0 字节接收），宿主稍后重试；**同步背压**，不丢数据。
3. io 线程上的写任务排空 ring：`TcpStream::write_all` / `KcpStream::write_all` / `smux Stream` write。KCP 侧受益于 kcp-rs 既有的 inline-send + `pending_flush` 批量冲刷路径（一次 burst 一次 flush）。

**接收（网络 → 宿主）**
1. 每条 conn 一个读任务（`knet::spawn_task`）: 阻塞读 → 追加进该 conn 的 **recv ring**（默认 256 KiB）→ 置位"有数据"。
2. 向事件环投递 `NC_EVENT_DATA(conn)`。**Data 事件只是"去读"的提示，可合并**：同一 conn 已有未消费 Data 事件时不重复投递 → 事件环天然不会因数据洪泛溢出。
3. 宿主收到事件后循环 `nc_recv(conn, buf, cap)` 直到返回 0（无数据）或负值（错误/关闭）。**内存所有权永不出 FFI**：宿主给 buffer，Rust 拷出。
4. recv ring 满：读任务暂停等待（背压传导到 TCP 窗口/KCP `rmt_wnd`/SMUX 流控），不丢数据。

**关闭**: `nc_close` 幂等 → conn 进入 `Closing` → 写任务排空后关闭 → 事件环投递 `NC_EVENT_CLOSED` → slab 槽位保留 generation，句柄不复用错乱（§7.3）。

### 4.4 同步/异步分层边界（核心问题：Rust async 怎么跨 FFI）

**原则：async 只存在于库内部的 io 线程上；FFI 边界和宿主侧 100% 同步。C ABI 没有 async 语义（C 的调用方拿不到 future），Unity/Godot 的主线程也不是异步执行器——所以 runtime 必须完全自包含在库里。**

```
宿主线程 (Unity main / Godot main)          netclient-io 线程 (库自建, 每 Client 1 条)
────────────────── 纯同步世界 ──────────    ──────────────── 异步世界 (runtime context) ────────────────
nc_dial / nc_send / nc_recv / nc_poll       block_on_local(run_actor())
      │ 同步 Rust facade                            ├─ actor 循环: select! { cmd / ring释放 }
      │ (查表 + memcpy + 出入队)                     ├─ 每连接: 读任务 + 写任务 (knet::spawn_task)
      ▼                                             ├─ KcpStream 内部 flush/input 循环 (builder 自动 spawn)
  命令 mpsc ──────────────── unbounded_send ──────► │ ├─ SMUX keepalive 定时器 (knet::sleep_ms)
  事件环  ◄──────────────── EventQueue.push ─────── │ ├─ 拨号任务 (DNS 经 cpu_block, 不阻塞 io 线程)
  收发 ring (每 conn, 双方触碰) ◄────────────────►  │ └─ CPU 密集: kcrypt/snappy → knet::cpu_block 线程池
                                                    │     (io 线程永不阻塞, 见 §4.6)
                                                    └─ 空闲时 runtime park 在 epoll_wait (零 CPU, 省电)
```

三个被否决的备选方案（记录决策理由）:

| 备选 | 否决原因 |
|---|---|
| 每个 `nc_*` 在宿主线程 `block_on` | 宿主主线程被卡住；且逐调用建 runtime 代价大 |
| 用 knet 的全局多线程 runtime（`knet::block_on` 的 `GLOBAL_RT`） | 默认 worker = num_cpus ≈ 手机 8 条常驻线程；生命周期不受控（永不销毁），无法在 `nc_client_free` 时干净回收 |
| 每个 conn 一个 runtime | 线程爆炸；连接数 = 线程数 |

选自建单线程 + `block_on_local` 的收益：**生命周期可控**（free 时 join 回收）、**省电**（空闲 park 在 epoll_wait，KCP 空闲连接的 flush 循环是 absolute-deadline 零定时器驻留）、**无跨线程数据竞争**（所有 async 状态单线程）。

### 4.5 io 线程上到底跑什么（actor 循环）

```rust
// core/src/client.rs —— io 线程唯一入口
fn io_thread_main(ctx: Arc<Ctx>) {
    knet::block_on_local(run_actor(ctx));      // current-thread runtime, 专职线程
}

async fn run_actor(ctx: Arc<Ctx>) {
    loop {
        tokio::select! {
            cmd = ctx.cmd_rx.recv() => match cmd {          // 宿主线程 unbounded_send 唤醒
                Some(Cmd::Dial(o))    => dial(&ctx, o).await,  // 成功后 spawn_task 读/写任务
                Some(Cmd::Close(id))  => close_conn(&ctx, id).await,
                Some(Cmd::Shutdown) | None => break,
            },
            // 读任务因 recv ring 满背压挂起时, 宿主 nc_recv 腾出空间会 Notify 唤醒它们
        }
    }
    shutdown_sequence(&ctx).await;             // §4.7
}
```

要点:
- `knet::spawn_task`（`tokio::spawn` 封装）在 `block_on_local` 的 context 里调用时，任务落在**同一条 io 线程**上——所有 async 状态天然无竞争。
- KCP 侧经 builder（`spawn_flush_loop`/`background_input`）自动 spawn 的内部循环同样落在本线程；SMUX keepalive、KCP `interval` 定时器都由 runtime 的 timer 驱动。
- **唤醒路径**: 宿主 `nc_dial/nc_close` → `unbounded_send`（非阻塞，且会唤醒 park 中的 runtime）；网络事件 → epoll/kqueue 就绪；定时 → timer wheel。没有轮询、没有自旋。

### 4.6 每个 `nc_*` 的阻塞语义（宿主线程纪律）

| API | 阻塞? | 耗时上界 | 说明 |
|---|---|---|---|
| `nc_client_new` | 否 | ms 级 | 仅启动线程 |
| `nc_dial` | 否 | O(参数拷贝) | 入命令队列, 立即返回句柄 |
| `nc_dial_blocking` | **是** | `timeout_ms` | Condvar 等结果（§4.8），加载页专用 |
| `nc_send` | 否 | O(len) memcpy | ring 满返回 `NC_EWOULDBLOCK`, 同步背压 |
| `nc_recv` | 否 | O(cap) memcpy | 无数据返回 0 |
| `nc_poll` | 否 | O(事件数) | 纯出队 |
| `nc_wait_event`（可选） | **是** | `timeout_ms` | Condvar 等事件, 给低帧率/专用泵线程用 |
| `nc_close` | 否 | O(1) | 入队, 状态立即置 `Closing` |
| `nc_client_free` | **是** | 排空+join | 建议退出流程/非主线程调用 |

`nc_send/nc_recv/nc_poll` 均线程安全（注册表 Mutex + 每 conn 独立 ring），但文档约定典型用法是单主线程调用。

### 4.7 关停序列（防 hang 是重点）

```
nc_client_free(client)
 1. 入队 Cmd::Shutdown → actor 收到后:
 2. abort 全部 conn 的读写任务 (knet JoinHandle::abort)
    → 任务在 await 点被取消, 持有的 socket/stream 随 drop → fd 必然关闭
    (不依赖"等 read 返回错误"——那是 hang 的经典来源)
 3. actor return → block_on_local 返回 → current-thread runtime drop
 4. std::thread::join
 5. drop Ctx (命令队列/注册表/事件环)
注意: 不做优雅排空。要"发完再退"的宿主应先自行确认数据已被对端消费
     (v2 可加 nc_flush(conn, timeout)); 事件环中未 pump 走的 CLOSED 随 Ctx 丢弃。
```

### 4.8 异步拨号、阻塞拨号与宿主侧 async/await

- `nc_dial`（推荐）: 入队即返回，结果经 `NC_EV_CONNECTED` / `NC_EV_DIAL_FAILED` 事件送达——与数据路径同一条事件流。
- `nc_dial_blocking`: Dial 命令携带一个 `Arc<(Mutex<Option<Result>>, Condvar)>` 回执槽；拨号任务完成后填充并 notify。**不经过事件环**，因此不会与 `nc_poll` 抢事件。仅限加载页/工具脚本。
- 宿主想要 async/await 语法糖？在**宿主侧**包一层，库不参与:

```csharp
// Unity: 基于 Pump 的 Task 包装 (UniTask 同理)
public Task<NetConnection> DialAsync(string url, NcDialOpts o) {
    var tcs = new TaskCompletionSource<NetConnection>();
    _pending[handle] = tcs;                 // Pump() 收到 CONNECTED/FAILED 时 SetResult
    nc_dial(_handle, url, ref o);
    return tcs.Task;
}
```

Godot 侧天然等价物是信号 + `await`（§9.1 的 `connected`/`data`/`closed` 信号）。两种引擎的"异步体验"都由事件泵派生，Rust 侧零改动。

### 4.9 "knet-only" 规则在本库的解释

I/O（连接/读写/定时/任务 spawn）全部经 `knet::*`（`TcpStream`/`UdpSocket`/`spawn_task`/`sleep_ms`/`block_on_local`/`cpu_block`）。actor 内部的**进程内同步原语**（命令队列 `tokio::sync::mpsc`、背压 `tokio::sync::Notify`、拨号回执 `Mutex+Condvar`）不涉及任何 I/O 后端——用 `tokio::sync` 不违背"knet-only 禁 raw tokio I/O"的本意；若要求字面 100% 无 tokio 字样，可换 `crossbeam-channel` + 定期 tick，代价是唤醒延迟。已在 §16 列为待拍板项。

---

## 5. 拨号矩阵（URL → 现有 API 的精确映射）

### 5.1 URL 语法

```
scheme://host:port[?k=v&...]

tcp://host:port[?timeout=5000]
kcp://host:port?crypt=aes&key=SECRET&mode=fast2&mtu=1350&sndwnd=512&rcvwnd=512
        &nodelay=1&interval=10&resend=2&nc=1&ds=10&ps=3&timeout=5000
kcptun://host:port?crypt=aes&key=SECRET&nocomp=0&mtu=1350&sndwnd=512&rcvwnd=512
        &ds=10&ps=3&keepalive=10&maxstreams=...&qpp=0&timeout=5000
```

参数同时可经 `nc_dial_opts_t` 结构体传入（结构体优先级高于 URL，URL 便于调试）。**key 一律传口令字符串，库内做 PBKDF2-HMAC-SHA1 / salt "kcp-go" / 32B 派生**（与 kcptun 线兼容的既有派生逻辑）。

### 5.2 映射表

| scheme | 构造路径（现有公开 API） | 备注 |
|---|---|---|
| `tcp://` | `knet::TcpStream::connect("host:port")`（内部含 DNS + 连接超时, `knet-rs/src/net/tokio.rs:551`） | 最短路径 |
| `kcp://` 无加密 | 远端解析(`SocketAddr`) → `knet::UdpSocket::connect(0.0.0.0:0, remote)` → `Arc<DatagramSocket>`（它实现了 `PacketTransport`）→ `KcpStream::with_transport(transport, remote).mtu(..).sndwnd(..).rcvwnd(..).set_kcp_nodelay(1,10,2,1).fec(ds,ps).build()` | `conn.rs:200`/`transport.rs` |
| `kcp://?crypt=..` | 同上，但 transport 换成 `Arc::new(CryptoTransport::new(sock, &key, method))` → `KcpStream::with_transport(crypto, remote)...` | `kcp_transport.rs:90`；`crypt="null"` = 无加密无头（区别于 `none`） |
| `kcptun://` | `UdpSocket::connect` → `KcptunSession::connect(Arc<DatagramSocket>, remote, &key, crypt, &KcptunConfig{kcp, smux, nocomp, ..})` → Client 会话；流 = `session.open_stream()` | `kcptun_session.rs:111,267`；Snappy/SMUX 由 session 内部完成，与 Go kcptun 服务端互通 |

实现注意:
- **DNS**: 游戏多为 IP 直连；域名时在拨号任务里用 `knet::cpu_block(|| std::net::ToSocketAddrs)` 解析，避免阻塞 io 线程（std 域名解析不算"raw tokio"）。
- **KCP conv/token**: 服务端首先下发决定 conv，builder 默认 `adopt_conv`/被动接受即可（kcp-go 语义）；`kcp://` 服务端可以用本仓库 `KcpListener`（sharded.rs）或 Go kcp-go，均线兼容。
- **FEC**: `ds/ps` 都 >0 才启用（`KcpConfig` 默认 0 = FEC off, `kcp-rs/src/config.rs:74-79`），移动端建议 10/3 起步，按丢包率调。
- `kcptun://` 场景下 SMUX `Config`（keepalive_interval/timeout、max_frame_size、max_receive_buffer、max_stream_buffer）透传为 URL 参数/opts 字段。

### 5.3 连接对象统一视图

```rust
enum ConnKind {
    Tcp(knet::TcpStream),
    Kcp(kcp_rs::KcpStream),
    Mux { session: Arc<KcptunSession>, stream: Arc<smux_rs::Stream> },
}
// 统一能力: send(&[u8]) / recv(&mut [u8]) / close() / stats()
// 读任务统一按 kind 分派; KcpStream 用其 inherent async read/write_all
```

---

## 6. netclient-core 内部设计

```
core/src
├── lib.rs          # Client/ConnHandle 公开 Rust API（FFI/GDExtension 都吃这层）
├── config.rs       # ClientConfig / DialOptions / UrlParser(url→DialOptions)
├── client.rs       # Client: 命令 mpsc、Registry、EventQueue、actor 循环
├── conn.rs         # ConnState(Dialing/Connected/Closing/Closed) + ConnKind + 收发 ring
├── dial.rs         # §5.2 的三条拨号路径（全部 async, 可注入 transport 便于测试）
├── event.rs        # Event{Connected/Data/Closed/DialFailed/Error} + 有界环
└── stats.rs        # per-conn 快照: rtt/srtt/rto/snd_queue/retrans...
```

- **Registry**: `Mutex<Slab<ConnEntry>>`，`conn_id = (slot:u32 << 32) | generation:u32`。关闭只置状态不回收槽；宿主再拿到旧 id 查表 generation 不匹配 → `NC_EBAD_HANDLE`。杜绝 ABA。
- **EventQueue**: `Mutex<VecDeque<Event>>`，cap 1024；`Data` 可合并（§4.3），`Closed/DialFailed/Error` 不可合并——若 ring 满则把最老的可合并 Data 挤掉并计数（`dropped_data_events` 统计），保证关停类事件不丢。
- **命令**: `Dial(opts) / Close(conn) / Shutdown`；`send/recv` 不进队列（直接操作 ring，宿主线程完成，§4.3）——只有真正需要 io 线程的才排队。
- **可测试性**: `dial.rs` 的 transport 构造抽象成 trait `DialTransport`，单测里用内存管道把两个 core 实例对连，跑全链路（含 ring/事件/关闭语义），不依赖网络。

---

## 7. C ABI 设计（netclient-ffi）

**API 核心契约 —— 同步、非阻塞、数据就绪语义（整个库只有这一种使用方式）**:

- 所有 `nc_*` 调用立即返回，宿主线程**永不等待网络**；等待（socket 就绪、KCP 重传定时器、keepalive、DNS）全部发生在库内部 io 线程上（§4）。
- `nc_recv`: 有数据 → 返回字节数 (>0)；**没数据 → 返回 0（正常返回，不是错误）**；关闭/错误 → 负值错误码。ring 里没读完的下次再读，不丢。
- `nc_send`: 接受进 ring → 返回接受字节数；ring 满 → `NC_EWOULDBLOCK`（下一帧重试，不排队不阻塞）。
- `nc_poll`: 没事件 → 返回 0。
- 宿主唯一职责：**每帧调一次 `nc_poll` + 收发**。没有回调、没有 await、没有隐藏阻塞。
- `nc_dial_blocking` / `nc_wait_event` 是**可选便利层，不属于核心契约**（低帧率工具/加载页可选用），v1 可不交付；核心 API 本身已经是纯非阻塞。

### 7.1 头文件草案 `netclient.h`

```c
#ifndef NETCLIENT_H
#define NETCLIENT_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#define NC_ABI_VERSION 1

typedef struct nc_client nc_client_t;      /* 不透明句柄 */
typedef uint64_t nc_conn_t;                /* (slot<<32)|generation */

/* ---- 错误码: >=0 成功(字节数等), <0 失败 ---- */
enum {
    NC_OK = 0, NC_EWOULDBLOCK = -1, NC_EINVAL = -2, NC_EBAD_HANDLE = -3,
    NC_ENOT_CONNECTED = -4, NC_ECLOSING = -5, NC_ETIMEDOUT = -6,
    NC_EREFUSED = -7, NC_EHOSTUNREACH = -8, NC_ENETDOWN = -9,
    NC_EQUEUE_FULL = -10, NC_ENOMEM = -11, NC_EURL = -12,
    NC_ECRYPT = -13, NC_EPANIC = -14, NC_EABI = -15 /* abi 不匹配 */
};

/* ---- 事件 ---- */
enum { NC_EV_CONNECTED = 1, NC_EV_DATA = 2, NC_EV_CLOSED = 3,
       NC_EV_DIAL_FAILED = 4, NC_EV_ERROR = 5 };
typedef struct nc_event {
    int32_t  type;
    int32_t  code;        /* NC_EV_CLOSED 的原因码 / 错误码 */
    uint64_t conn;
    uint32_t nbytes;      /* 预留 */
    uint32_t _pad;
} nc_event_t;

/* ---- 配置 ---- */
typedef struct nc_config {
    uint32_t struct_size;      /* 必填 = sizeof(nc_config_t), 向前兼容 */
    uint32_t io_mode;          /* 0=current-thread(默认), 1=multi-thread */
    uint32_t recv_ring_bytes;  /* 0=默认 256<<10 */
    uint32_t send_ring_bytes;  /* 0=默认 512<<10 */
    uint32_t event_queue_cap;  /* 0=默认 1024 */
    void (*log_cb)(void* ud, int level, const char* msg);  /* 可空 */
    void*  log_ud;
} nc_config_t;

/* ---- 拨号参数 (0/NULL = 用默认) ---- */
typedef struct nc_dial_opts {
    uint32_t struct_size;
    uint32_t timeout_ms;        /* 拨号超时, 默认 5000 */
    /* KCP 通道参数 */
    uint32_t mtu;               /* 1350 */
    uint32_t sndwnd, rcvwnd;    /* 512 */
    uint32_t nodelay, interval, resend, nc;  /* 1,10,2,1 (fast2) */
    uint32_t fec_data, fec_parity;           /* 0,0 = FEC off */
    /* kcptun:// 额外 */
    int32_t  nocomp;            /* 0 = 启用 snappy(默认) */
    uint32_t smux_keepalive;    /* 秒, 默认 10 */
    uint32_t smux_version;      /* 1 或 2 */
    /* 加密 */
    const char* crypt;          /* "aes","aes-128","aes-128-gcm","salsa20","null","none".. */
    const char* key;            /* 口令(库内 PBKDF2 派生); NULL=明文 */
} nc_dial_opts_t;

/* ---- 生命周期 ---- */
int32_t        nc_abi_version(void);
nc_client_t*   nc_client_new(const nc_config_t* cfg);      /* 启动 io 线程 */
void           nc_client_free(nc_client_t* c);             /* 关全部 conn + join */

/* ---- 拨号: 异步发起, 立即返回句柄, 结果经 NC_EV_CONNECTED / NC_EV_DIAL_FAILED ----
   (非阻塞; 拨号等待发生在库内部 io 线程) */
nc_conn_t      nc_dial(nc_client_t* c, const char* url, const nc_dial_opts_t* opts);
/* 可选便利层(不属于核心契约): 阻塞至连接建立/失败, 仅加载页/工具用 */
nc_conn_t      nc_dial_blocking(nc_client_t* c, const char* url,
                                const nc_dial_opts_t* opts, int32_t* out_err);

/* ---- 数据: 全部非阻塞 ---- */
int32_t        nc_send(nc_client_t* c, nc_conn_t conn, const uint8_t* buf, uint32_t len);
int32_t        nc_recv(nc_client_t* c, nc_conn_t conn, uint8_t* buf, uint32_t cap);
int32_t        nc_poll(nc_client_t* c, nc_event_t* out, uint32_t max);  /* 返回事件数 */
int32_t        nc_wait_event(nc_client_t* c, nc_event_t* out, uint32_t max, uint32_t timeout_ms); /* 可选: 阻塞等待(Condvar), 低帧率循环用 */
void           nc_close(nc_client_t* c, nc_conn_t conn);
int32_t        nc_stats(nc_client_t* c, nc_conn_t conn, nc_stats_t* out);
const char*    nc_version(void);

#ifdef __cplusplus
}
#endif
#endif
```

ABI 纪律:
- `struct_size` 模式：未来加字段只追加在结构体尾部，旧宿主传旧 `sizeof` 仍合法（Rust 侧按 `struct_size` 裁剪）。
- 所有 `nc_*` 入口 `catch_unwind`；指针非空校验；字符串仅在本调用周期内有效（除 `log_ud`）。
- `nc_send/nc_recv` 保证 O( memcpy )，无 syscall、无锁竞争路径跨线程（ring 是每 conn 独立 + actor 单线程）。

### 7.2 生命周期状态机

```
nc_dial ──► Dialing ──ok──► Connected ──nc_close/网络错──► Closing ──排空──► Closed(事件)
                │                                           
                └──fail/timeout──► NC_EV_DIAL_FAILED(conn 无效)
```

宿主侧典型循环（每帧）:

```c
nc_event_t ev[32];
int n = nc_poll(client, ev, 32);          /* 0 = 本帧无事件, 正常 */
for (int i = 0; i < n; i++) {
    switch (ev[i].type) {
    case NC_EV_CONNECTED: mark_connected(ev[i].conn); break;
    case NC_EV_DATA: {
        uint8_t buf[16*1024]; int r;
        while ((r = nc_recv(client, ev[i].conn, buf, sizeof buf)) > 0)
            feed_protocols(ev[i].conn, buf, r);
        /* r == 0: 读空了(正常); r < 0: 错误/已关闭 */
        break; }
    case NC_EV_CLOSED: schedule_reconnect(ev[i].conn); break;
    }
}
```

数据事件的消费也可以不走事件: 每帧直接对活跃连接 `while ((r = nc_recv(...)) > 0)` 拉空即可——`NC_EV_DATA` 只是"该去读了"的提示，可合并、可忽略。

### 7.3 句柄安全

`nc_conn_t` 带 generation；对端关闭后宿主若仍持有旧 id：`nc_send/nc_recv` 返回 `NC_EBAD_HANDLE`（或 `NC_ECLOSING`），绝不指向新连接。

### 7.4 统计 `nc_stats_t`

```c
typedef struct nc_stats {
    uint32_t struct_size;
    uint64_t tx_bytes, rx_bytes;
    uint32_t rtt_ms, srtt_ms, rto_ms;
    uint32_t snd_queue_len, snd_queue_bytes;
    uint32_t retrans_pkts;          /* KCP */
    uint32_t dropped_data_events;   /* 事件环挤压 */
    uint64_t fec_recovered;         /* ds/ps>0 时 */
} nc_stats_t;
```

实现来源: KCP 内部 rtt/rto/snd 队列需要 kcp-rs 增加**只读 getter**（additive, 非 breaking，见 §12）; TCP 侧取 sockbuf 统计; 其余 core 自记。

### 7.5 日志

`nc_config_t.log_cb` → 内部用同一回调实现 `log`/`tracing` 风格宏（核心层仍用 `log` crate，FFI 层装 bridge）。默认级别 `warn`；真机排障时宿主可开 `debug`。

### 7.6 可选回调 API（二期）

`nc_set_callbacks(client, {on_connected, on_data, on_closed}, ud)`——明确文档化：回调发生在 `netclient-io` 线程，禁止调用引擎主线程 API；仅推荐 Godot C#/服务端工具使用。v1 只交付 pump。

---

## 8. Unity 集成

### 8.1 交付物（Unity Package 布局）

```
Assets/Plugins/NetClient/
├── Runtime/
│   ├── NetClient.cs          // P/Invoke 封装(下) + NetConnection 类 + 事件分发器
│   └── NetClient.Runtime.asmdef
├── Android/
│   ├── arm64-v8a/libnetclient.so
│   ├── armeabi-v7a/libnetclient.so
│   └── x86_64/libnetclient.so          // 编辑器/模拟器
└── iOS/
    └── libnetclient.a                  // Unity 会自动把它挂进 Xcode 工程
```

### 8.2 C# 封装要点

```csharp
#if UNITY_IOS && !UNITY_EDITOR
    private const string Lib = "__Internal";          // 静态库
#else
    private const string Lib = "netclient";
#endif
    [DllImport(Lib)] private static extern IntPtr nc_client_new(ref NcConfig cfg);
    [DllImport(Lib)] private static extern ulong  nc_dial(IntPtr c, string url, ref NcDialOpts o);

public sealed class NetClient : IDisposable {
    // 每帧由 MonoBehaviour.Update() 调 Pump():
    public void Pump() {
        int n = nc_poll(_handle, _evBuf, (uint)_evBuf.Length);
        for (...) Dispatch(ev);        // 主线程内分发 C# event —— 天然线程安全
    }
    public NetConnection Dial(string url, NcDialOpts o) { ... }
    public void Dispose() { nc_client_free(_handle); _handle = IntPtr.Zero; }
}
```

注意事项:
- **iOS**: `DllImport("__Internal")`；`libnetclient.a` 放 `Plugins/iOS` 会被 Unity 自动链接；若 Xcode 报符号缺失，给 Other Linker Flags 加 `-force_load $(PROJECT_DIR)/Libraries/Plugins/iOS/libnetclient.a`。
- **Android**: IL2CPP 下 `DllImport("netclient")` 自动 `System.loadLibrary`；`.so` 需在 Plugin Inspector 里按 ABI 勾选平台；INTERNET 权限由 manifest 常规处理。
- **GC**: v1 是 pump 式，无跨线程 delegate，**不存在 delegate 被 GC 回收的坑**；若启用回调 API（§7.6），delegate 必须 `GCHandle.Alloc` 持久持有。
- IL2CPP 需在 `link.xml`/托管剥离设置里保留 `NetClient` 程序集（防 stripping 裁掉 P/Invoke）。

---

## 9. Godot 集成

两条路线，**主推 GDExtension**：

### 9.1 GDExtension（Godot 4.x, 主线）

`netclient/godot/` 直接依赖 `netclient-core`（Rust→Rust，零 FFI 开销），暴露：

```
NetClient (Node)     # 属性: io_mode/ring 大小; 方法: dial(url, opts) -> NetConnection
                     # 信号: connected(conn) / data(conn) / closed(conn, code) / dial_failed(conn, code)
NetConnection (RefCounted)  # send(bytes) -> int / recv() -> PackedByteArray / close()
```

```gdscript
# 游戏侧
var nc := NetClient.new(); add_child(nc)
var conn := nc.dial("kcp://1.2.3.4:7777?crypt=aes&key=SECRET&ds=10&ps=3", null)
conn.data.connect(_on_data)
# _process 里: var chunk := conn.recv()  (或直接用信号)
```

- 平台产物由 `.gdextension` 描述：`android.arm64 = res://bin/libnetclient.android.template_release.arm64.so`、`ios.ios = ...xcframework`、`windows/macos/linux` 各一条；iOS 上 GDExtension 用静态链接（`entry_symbol` 同静态库方案）。
- 信号在主线程 `_process` pump 时发射（core 的 EventQueue → Node.emit_signal），同样规避跨线程改场景树。

### 9.2 C# P/Invoke（Godot .NET, 备用）

Godot 4 .NET 与 Unity 共享同一套 `NetClient.cs`（把 `DllImport` 常量改成 "netclient" 即可），零额外维护成本——这也是 C ABI 先行的红利。

### 9.3 交付物

```
addons/netclient/
├── netclient.gdextension
├── bin/ (android arm64/v7a/x86_64 .so, ios xcframework, win .dll, macos .framework, linux .so)
└── scripts/netclient.gd (可选 GDScript 薄糖)
```

---

## 10. 移动网络鲁棒性

1. **存活判定**: 三套 scheme 各有依据——TCP: 读/写错误; KCP: kcp-go v5 语义的 `dead_link`（连续重传失败上限，端口行为与 Go 一致）+ 显式 keepalive 应用层心跳（可选 `nc_send` ping 包, 业务层做更好）; `kcptun://`: SMUX keepalive（`smux_keepalive` 参数, 默认 10s interval / 30s timeout）。
2. **重连策略 v1 = 宿主驱动**: `NC_EV_CLOSED` → 引擎按自己的退避策略再 `nc_dial`（游戏要处理"回到大厅/换角色"等语义，库不该抢）。core 预留 `ReconnectPolicy`（指数退避 + jitter + 上限次数）做 v2 的自动重连开关。
3. **网络切换 (Wi-Fi↔蜂窝)**: 旧 socket 必死，KCP conv 换新=新连接。文档明确：上层用"连接 token + 会话重入"（业务层）或未来 `nc_rebind`（v2, 需要 kcp-rs 支持迁移 conv，暂不做）。
4. **前后台**: iOS/Android 挂起后 socket 会死——`applicationDidEnterBackground` 时宿主应主动 `nc_close` + 回前台重拨；库不自动检测（避免误判）。
5. **电量**: current-thread runtime + `interval=10ms` 的 KCP flush 属正常功耗；提供 `interval` 调大到 20–40ms 的省电档文档。

---

## 11. 加密与兼容性

- 加密构造统一走 `kcrypt_rs::select_block_crypt`/`select_aead_crypt`（经 `CryptoTransport`），算法集合与 kcptun 一致（AES-CFB/GCM、Salsa20、SM4 等 13 种 BlockCrypt + AES-128-GCM AEAD）。
- 线格式不动: CFB `[nonce16][crc32][payload]`、GCM `[nonce12][ct+tag16]`、`null` 无头——**与 Go kcptun/kcp-go 互通由既有 e2e 保证**。
- `kcp://?crypt=..` 是"裸 KCP + CryptoTransport"，服务端要么是本仓库生态，要么是实现了同样线格式的对端；**这不是 Go kcptun 服务器接受的格式**（那个用 `kcptun://`）。文档必须写清这个区别，防误用。
- QPP（可选）: `kcptun://?qpp=1&qppkey=..` 时按 `kcptun-client/src/client.rs:195` 的同款层次（QPPPort 包在 SMUX 之外）实现；默认关。
- 密钥派生（PBKDF2/salt "kcp-go"/32B）在库内完成，宿主永远只传口令。

---

## 12. 对既有 crate 的增量改动（全部 additive）

| 改动 | 理由 | 风险 |
|---|---|---|
| kcp-rs: `KcpStream` 增加只读 getter（`srtt_ms/rto_ms/snd_queue_len/retrans_count` 等快照） | `nc_stats_t` 需要 per-conn 数据；现全局 snmp 不够 | 无: 新 pub 方法, 不动线协议; 版本 minor bump |
| kcptun-common: `KcptunSession` 暴露 `session_stats()`（SMUX 层计数） | 同上 | 无 |
| 主 Makefile: 增加 `netclient-android / netclient-ios` 目标（或独立 `netclient/Makefile`，推荐后者） | 构建入口 | 无 |
| AGENTS.md: workspace 表格加一行 netclient（结构变更后的同步义务） | 仓库规则 | 无 |

不改动: 任何线格式、`kcp.rs` 状态机、`sharded.rs`、发布 crate 的既有签名。

---

## 13. 构建与发布

### 13.1 目标矩阵与命令

```bash
# Android (cargo-ndk, 一次产 3 ABI 并按 Unity/Godot 布局落盘)
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 -o ../Assets/Plugins/Android \
    build --release -p netclient-ffi

# iOS (设备 + 模拟器 → xcframework)
cargo build --release --target aarch64-apple-ios     -p netclient-ffi   # staticlib
cargo build --release --target aarch64-apple-ios-sim -p netclient-ffi
xcodebuild -create-xcframework -library target/aarch64-apple-ios/release/libnetclient.a ...

# 桌面 (编辑器 Play Mode)
cargo build --release -p netclient-ffi   # host triple, cdylib
```

- `rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios aarch64-linux-android armv7-linux-androideabi x86_64-linux-android`
- Android: NDK r26+（cargo-ndk 自动找 clang）; iOS: Xcode 15+（bitcode 已废，不设置）。
- `opt-level`: 默认 3；提供 `release-mobile` profile（`opt-level="s"` + LTO）供体积敏感场景，体积差实测后定稿。

### 13.2 CI

新增 job: (a) `cargo test -p netclient-core`（含内存管道全链路测试）; (b) `cargo build -p netclient-ffi` 桌面三平台矩阵; (c) Android/iOS 产物构建 + 产物冒烟（Godot headless 加载 GDExtension 跑一个 loopback 拨号）。

### 13.3 版本

`NC_ABI_VERSION` 单调递增；`netclient-ffi` SemVer 独立于 kcp-rs；变更头文件必须 bump。头文件用 `cbindgen` 从 `ffi/src/api.rs` 生成（注释驱动），避免手写漂移。

---

## 14. 测试计划

1. **core 单测**（无网络）: URL 解析参数优先级; ring 水位/背压; 事件合并与关停事件不丢; 句柄 generation/ABA; 关闭排空语义。
2. **core 集成**（loopback）: 内存管道双 core 对连全链路; 本机 `127.0.0.1` 起 TCP/KCP server（复用 kcp-rs 测试基建）三类 scheme 收发/超时/断线; `kcptun://` 对接本仓库 `KcptunListener`。
3. **FFI 冒烟**: Rust 测试直接调用本 crate 导出的 `nc_*`（模拟 C 调用方）; C 写一个 100 行的 console demo 编进 CI。
4. **真机清单**: iOS 真机 + Android 真机各一台: 前后台切换、飞行模式开关、Wi-Fi↔蜂窝切换、10 分钟空闲 keepalive 存活、1MB/s 持续下行 5 分钟内存稳定（recv ring 不增长）。
5. **互通回归**: `kcptun://` 与 Go kcptun 服务端的互通复用 `test_e2e.sh` 思路——**该脚本按仓库规则需要用户确认后才能跑**，方案内仅建议不自动执行。
6. **基准**: 拨号延迟、单 conn 吞吐、1k msg/s 小包 RTT（手机上跑 5 分钟，记录能耗主观档位）。

---

## 15. 里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| M1 | netclient workspace 骨架; core: Client/actor/事件环/ring/Registry; `tcp://` 拨号; FFI: 句柄+poll+send/recv; 桌面 cdylib + C demo | loopback 测试全绿; C demo 收发 |
| M2 | `kcp://`（含 crypt/opts 全参数）; kcp-rs stats getter; 与 kcp-rs KcpListener 互通测试 | 参数矩阵测试; RTT 统计可见 |
| M3 | `kcptun://`（KcptunSession + QPP 可选）; 统计透传 | 对接本仓库 KcptunListener 收发; （可选, 经确认）对 Go kcptun e2e |
| M4 | cargo-ndk / iOS 构建; Unity 包 + Sample 场景（Chat/Echo）; 真机清单跑通 | iOS/Android 真机通过 §14.4 |
| M5 | Godot GDExtension + demo; .NET C# 路线验证 | Godot 4 双平台加载并跑通 |
| M6 | 鲁棒性打磨: 退避重连工具类、前后台文档、省电档参数、stats 完善、cbindgen 进 CI | 文档齐; NC_ABI_VERSION=1 冻结 |

工作量直觉: M1≈1 周, M2≈1 周, M3≈3–5 天, M4≈1 周(真机联调浮动大), M5≈3 天, M6 持续。

---

## 16. 风险与开放问题

1. **`KcpStream` 不是 `AsyncRead/AsyncWrite`**（inherent async read/write, `conn.rs:745,843`）——`kcptun://` 路径已由 `KcptunSession` 内部解决，core 对 `kcp://` 直接用 inherent API 即可；不引入新适配层。
2. **panic=abort 双 workspace 的维护成本**: path dep 跨根合法，但 CI 要防有人把 netclient 误挂进主 workspace（加 CI 检查 `cargo metadata` 根成员）。
3. **iOS 静态库链接顺序**: 若 Unity 生成工程链接失败，`-force_load` 兜底；需在真机 M4 首日验证。
4. **kcp-rs stats getter 的字段可得性**: KCP 状态机内部字段是否全部可读需实现时核对（只加 getter，不动状态机）。
5. **多 Client 实例 = 多 io 线程**: 游戏通常 1 个足够；文档默认单例。
6. **开放问题（实现前拍板）**:
   - `release-mobile` 是否需要 `opt-level="s"`（先 A/B 体积/性能）；
   - Godot 最低支持版本定 4.2+ 还是 4.3+；
   - 回调 API（§7.6）是否进 v1（建议不进）；
   - actor 内部同步原语（`tokio::sync::mpsc`/`Notify`）是否算"raw tokio"（§4.9 的解释是否成立，需规则制定者确认；替代方案 crossbeam + tick 有唤醒延迟代价）。
