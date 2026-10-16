<div align="center">
 <img align="absmiddle" src="docs/logo.png" alt="kcptun-rs logo" width="256" height="256"> 

  <p><strong>Rust 移植 kcptun — 性能最高达到 Go 版本的 5.38 倍，完全线上兼容</strong></p>

  <p>
    <img src="https://img.shields.io/badge/build-passing-brightgreen" alt="Build Status">
    <img src="https://img.shields.io/badge/tests-551%20passed-brightgreen" alt="Tests">
    <img src="https://img.shields.io/badge/e2e-68%20passed-brightgreen" alt="E2E">
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
    <img src="https://img.shields.io/badge/rust-1.92+-orange" alt="Rust">
    <img src="https://img.shields.io/badge/Go%20compat-v5-success" alt="Go Compatible">
  </p>

  <p><a href="README.md">English</a> | 中文</p>

  <table align="center">
    <tr>
      <td align="center" width="25%"><b>5.38×</b><br><sub>相对 Go 最高加速</sub></td>
      <td align="center" width="25%"><b>68 / 68</b><br><sub>Go↔Rust 端到端测试</sub></td>
      <td align="center" width="25%"><b>551</b><br><sub>测试用例</sub></td>
      <td align="center" width="25%"><b>~2 MB</b><br><sub>strip 后二进制</sub></td>
    </tr>
  </table>
</div>

<details>
<summary><b>免责声明</b> — 本项目是 Vibe Coding 移植测试，仅供学习交流使用。</summary>

本项目是一次 **Vibe Coding**（利用 AI 辅助编程的实践方式）的移植测试——通过尝试移植现有代码库来实践 AI 辅助编程。核心是探索和验证 Vibe Coding 这套工作流本身，而非专门做一个生产级软件移植。本项目**不是**生产级软件，不保证功能正确性、稳定性和安全性。

**严禁用于任何违法违规用途**，包括但不限于翻墙、非法数据传输、网络攻击等。使用者的任何违法行为均与本项目及作者无关，由行为人自行承担全部法律责任。

完整免责声明请参阅 [DISCLAIMER.zh.md](DISCLAIMER.zh.md)。
</details>

## 📑 目录

<p align="center">
  🔥 <a href="#-性能概览">性能概览</a> ·
  📖 <a href="#-什么是-kcptun-rs">什么是 kcptun-rs？</a> ·
  ✨ <a href="#-功能特性">功能特性</a> ·
  🔐 <a href="#-加密认证说明">加密认证说明</a> ·
  🚀 <a href="#-快速开始">快速开始</a> ·
  📊 <a href="#-性能深度分析">性能深度分析</a> ·
  📡 <a href="#-客户端io-模式对比">客户端 I/O 模式对比</a> ·
  🔗 <a href="#-go-兼容性">Go 兼容性</a> ·
  🏗️ <a href="#-架构">架构</a> ·
  🔧 <a href="#-构建与运行">构建与运行</a> ·
  🎛️ <a href="#-延迟调优指南linux">延迟调优指南（Linux）</a> ·
  🔬 <a href="#-优化历程">优化历程</a> ·
  🧪 <a href="#-测试严谨性">测试严谨性</a> ·
  📋 <a href="#-cli-选项">CLI 选项</a> ·
  💡 <a href="#-为什么选择-rust">为什么选择 Rust？</a> ·
  📝 <a href="#-许可证">许可证</a>
</p>

## 🔥 性能概览

kcptun-rs 在**几乎所有加密算法和压缩组合下都超越 Go kcptun**，同时保持**完全的线上兼容**——这意味着你可以将 Go kcptun 隧道的一端替换为 Rust 二进制文件，立即获得加速。

| 加密算法 | vs Go (macOS M1) | vs Go (Linux VPS) |
|----------|:---------------:|:-----------------:|
| **SM4** (无压缩) | **4.76倍** 🏆 | **1.87倍** |
| **SM4** (压缩) | **5.32倍** 🏆 | **3.17倍** 🏆 |
| **XOR** (无压缩) | **2.54倍** | **1.60倍** |
| **CAST5** (无压缩) | **1.98倍** | **1.04倍** |
| **Twofish** (压缩) | **1.69倍** | **1.63倍** |
| **AES-128** (无压缩) | **1.59倍** | **1.14倍** |
| **AES-128-GCM** (压缩) | **1.33倍** | **1.25倍** |
| **AES-128** (压缩) | **1.18倍** | **1.83倍** |
| **AES-128** 大吞吐 | **1.67倍** | **1.17倍** |

*macOS M1：10 连接 × 每连接 1 MB。Linux VPS：1 vCPU AMD EPYC-Rome，4 连接 × 每连接 1 MB。完整矩阵见下文。*

## 📖 什么是 kcptun-rs？

**kcptun** 是一个稳定、安全的 TCP-over-UDP 隧道工具，利用 [KCP](https://github.com/skywind3000/kcp)（快速 ARQ 协议）在高延迟或丢包网络环境下加速 TCP 流。它具备 SMUX 多路复用、Reed-Solomon 前向纠错（FEC）、Snappy 压缩以及可选的加密功能，全部整合在一个二进制文件中。

**kcptun-rs** 是用 Rust 完整重写的实现：

- ✅ **线上兼容** Go kcptun（kcp-go v5）— Rust ↔ Go、Go → Rust、Rust → Rust 全部可互通
- ⚡ **性能超越 Go** — 大多数加密/模式组合下都更快，macOS 最高达 **5.38 倍**，Linux VPS **1.11–1.59 倍**
- 🧩 **15 种加密方法**（含 AES-128-GCM）：AES、SM4、Salsa20、Blowfish、Twofish、CAST5、3DES、TEA、XTEA、XOR 等
- ⚡ **单一异步运行时**：tokio（多线程，高并发）
- 🎯 **生产级功能**：FEC、SMUX v1/v2、QPP 混淆、SNMP 统计、速率限制、pprof 性能分析
- 🔄 **跨平台**：macOS、Linux、ARMv7（树莓派）、ARM64（AWS Graviton）

## ✨ 功能特性

| 类别 | 详情 |
|------|------|
| **兼容性** | 与 Go kcptun（kcp-go v5）完全线上兼容 — 所有加密算法、模式、SMUX 版本、FEC、Snappy |
| **加密** | 15 种方法：`null`、`none`、`xor`、`aes-128`、`aes-192`、`aes`(256)、`aes-128-gcm`、`sm4`、`tea`、`xtea`、`salsa20`、`blowfish`、`twofish`、`cast5`、`3des`（别名：`aes-256`、`salsa`、`tripledes`）—— 见[加密认证说明](#-加密认证说明) |
| **KCP 模式** | `normal`、`fast`、`fast2`、`fast3` |
| **SMUX** | v1 & v2 多路复用 — 单条 KCP 连接承载多个 TCP 流 |
| **FEC** | Reed-Solomon 前向纠错（默认 10/3，与 Go 兼容） |
| **压缩** | 会话级 Snappy 压缩，与 Go 字节一致，默认开启 |
| **QPP** | 量子置换垫 — 可选的后量子流混淆层 |
| **运行时** | tokio（多线程） |
| **Go pprof** | `--pprof` 输出 Go 兼容的 protobuf 格式 → 可直接用 `go tool pprof` 分析 |
| **速率限制** | 每连接令牌桶限速（`--ratelimit`） |
| **SNMP 统计** | 与 Go 兼容的 SNMP 字段，零开销按需采集 |
| **交叉编译** | ARMv7（树莓派）、ARM64（Graviton）、Linux musl — 全部可从 macOS 编译 |
| **日志** | 结构化日志级别（RUST_LOG），支持文件日志 |

## 🔐 加密认证说明

只有 `aes-128-gcm` 对密文做了认证（AEAD）。其余后端都是 CFB（或流式 XOR）
加 CRC32 校验，**没有 MAC**——网络攻击者可以伪造或篡改数据包。这是 Go
kcptun 线缆格式的固有设计，改了就破坏互操作。

| 加密算法 | 有认证 | 说明 |
|----------|--------|------|
| `aes-128-gcm` | ✅ **有** | AEAD；**生产环境推荐** |
| `aes` / `aes-128` / `aes-192` 等 CFB 系 | ❌ 无 | 仅 CRC32 |
| `xor` / `none` / `null` | ❌ 无 | **仅供调试，禁止生产** |

生产环境请优先使用 `--crypt aes-128-gcm`。

## 🚀 快速开始

```bash
# 构建（优化发布版，含 LTO）
cargo build --release
# 二进制文件：target/release/kcptun-server、target/release/kcptun-client

# 启动服务端（监听 UDP :29900，转发到本地 HTTP :8080）
./target/release/kcptun-server -t "127.0.0.1:8080" -l ":29900" --key "my-secret"

# 启动客户端（监听 :12948，隧道到远程服务端）
./target/release/kcptun-client -r "server-ip:29900" -l ":12948" --key "my-secret"
```

现在将你的应用指向 `127.0.0.1:12948` — 所有 TCP 数据都将被加密、压缩并通过 KCP 加速传输到远程服务端。

### 使用配置文件

```bash
kcptun-client -c client.json
kcptun-server -c server.json
```

两份配置的字段集合**互不相通**：客户端专属字段（`localaddr`、`remoteaddr`、`conn`、`autoexpire`、`scavengettl`）会被服务端拒绝，服务端专属字段（`listen`、`target`、`shards`、`peripsessionrate`、`maxsessionsperip`）也会被客户端拒绝。

```json
{
    "localaddr": ":12948",
    "remoteaddr": "vps:29900",
    "key": "my-secret",
    "crypt": "aes-128",
    "mode": "fast2",
    "conn": 2,
    "sndwnd": 128,
    "rcvwnd": 512,
    "datashard": 10,
    "parityshard": 3,
    "nocomp": false,
    "smuxver": 2,
    "keepalive": 10,
    "autoexpire": 0,
    "scavengettl": 600
}
```

```json
{
    "listen": ":29900",
    "target": "127.0.0.1:12948",
    "key": "my-secret",
    "crypt": "aes-128",
    "mode": "fast2",
    "sndwnd": 1024,
    "rcvwnd": 1024,
    "datashard": 10,
    "parityshard": 3,
    "nocomp": false,
    "smuxver": 2,
    "keepalive": 10,
    "shards": 0
}
```

> ⚠️ **`--key`、`--crypt`、`--mode` 和 `--nocomp` 必须客户端与服务端一致。** 压缩默认开启。

**新增服务端参数：** `--peripsessionrate`（每 IP 新建会话速率，默认 20/s）、`--maxsessionsperip`（每 IP 并发会话上限，默认 0=不限）、`--pprofaddr`（pprof 监听地址，默认 `127.0.0.1:6060`）。

**L-4 决策记录：** RTT 仅在 `snd_buf` 命中时采样**有意不做**——Go kcp-go 对每个带合法时间戳的 ACK 都更新 RTT（在 `parse_ack` 之前），改动会偏离 RTO 行为并破坏互操作。详见 `AGENTS.md`。

**JSON 配置规则（审计加固）：**
- JSON 中的值优先于命令行参数。
- **未知字段直接报错**（`deny_unknown_fields`）——把 `"shards"` 写成 `"shard"` 会在启动时失败，而不是被静默忽略。
- 服务端专属字段 `"shards"` 已支持（语义同 `--shards`）；`--threads` 仅命令行提供（无 JSON 字段）。
- 非法值启动即失败：`--crypt` 必须是已知算法，`--mtu` ≥ 50，`--dscp` 0–63，`--conn` ≤ 256，`--shards` ≤ 64。

## 📊 性能深度分析

<details>
<summary><strong>大吞吐测试（macOS M1，200 MB，AES-128-CFB，无压缩）</strong></summary>

路径标签为 **Client → Server**（大流量由客户端发往服务端；见 `bench/run_bench.sh`）。

| 路径 (Client → Server) | 吞吐量 | 延迟 | vs Go→Go |
|------|:-----:|:----:|:--------:|
| **Go → Go** | 51.15 MB/s | 0.31 ms | 1.00× |
| **Rust-Tokio → Rust-Tokio** | **85.60 MB/s** 🏆 | **0.12 ms** | **1.67×** |
| Rust-Tokio → Go | 76.48 MB/s | 0.11 ms | 1.50× |
| Go → Rust-Tokio | 30.28 MB/s | 0.15 ms | 0.59× |

> Rust-Tokio 在 M1 主机上明显快于 Go→Go。

</details>

<details>
<summary><strong>完整加密 × 压缩矩阵</strong></summary>

测试：10 并发连接，每连接 1 MB，所有 30+ 轮次全部通过（0 失败）。

**无压缩**（`--nocomp`）：

| 加密算法 | Rust-Tokio | Go | R/Go |
|---------|:----------:|:--:|:----:|
| null | 38.4 | 35.5 | 1.08× |
| none | 29.4 | 39.2 | 0.75× |
| xor | 41.6 | 16.4 | **2.54×** |
| aes-128 | 43.4 | 27.2 | **1.59×** |
| aes-128-gcm | 36.6 | 41.5 | 0.88× |
| salsa20 | 35.8 | 32.3 | **1.11×** |
| blowfish | 31.5 | 28.6 | **1.10×** |
| twofish | 35.1 | 23.2 | **1.51×** |
| cast5 | 33.3 | 16.9 | **1.98×** |
| 3des | 14.5 | 11.8 | **1.23×** |
| tea | 38.2 | 31.7 | **1.20×** |
| xtea | 24.7 | 18.6 | **1.33×** |
| **sm4** | **16.7** | **3.5** | **4.76×** 🏆 |

**带压缩**（Snappy）：

| 加密算法 | Rust-Tokio | Go | R/Go |
|---------|:----------:|:--:|:----:|
| aes-128-gcm | 36.4 | 27.4 | **1.33×** |
| salsa20 | 29.0 | 20.1 | **1.44×** |
| **sm4** | **18.7** | **3.5** | **5.32×** 🏆 |
| twofish | 34.4 | 20.4 | **1.69×** |
| cast5 | 36.5 | 26.4 | **1.38×** |
| blowfish | 34.4 | 25.7 | **1.33×** |
| aes-128 | 31.3 | 26.5 | **1.18×** |

> **SM4 是最大亮点**：Rust 比 Go 快 4.6–5.4 倍，因为 Go 实现使用纯软件 S-box，而 Rust 受益于编译器的自动向量化和预计算查找表。

</details>

<details>
<summary><strong>Linux VPS 基准测试（1 vCPU，AMD EPYC-Rome）</strong></summary>

来自 Linux VPS（CentOS 8，1 vCPU / 2 线程，AMD EPYC-Rome @ 2.8 GHz，2 GB RAM）的结果 —— 正是 kcptun 常部署的低端云主机类型。测试前已调优内核 UDP 缓冲区（`net.core.rmem_max=4MB`）和 CFS 唤醒粒度（1 ms）（见下文[延迟调优指南](#-延迟调优指南linux)）。

**大吞吐测试（100 MB，AES，fast 模式，来自 `bench/run_bench.sh`）：**

| 路径 (Client → Server) | 吞吐量 | 延迟 | vs Go→Go |
|------|:-----:|:----:|:--------:|
| **Go → Go** | 51.27 MB/s | 0.27 ms | 1.00× |
| **Rust-Tokio → Rust-Tokio** | **59.91 MB/s** 🏆 | **0.20 ms** | **1.17×** |
| Rust-Tokio → Go | 57.94 MB/s | 0.23 ms | 1.13× |
| Go → Rust-Tokio | 64.36 MB/s | 0.20 ms | 1.26× |

**完整加密 × 压缩矩阵（4 连接 × 1 MB，来自 `bench/bench_linux_cmp.py`）：**

无压缩（`--nocomp`）：

| 加密算法 | Rust-Tokio | Go | R/Go | 胜者 |
|---------|:----------:|:--:|:----:|:----:|
| null | **66.2** | 49.8 | **1.33×** | Rust |
| none | **56.6** | 41.7 | **1.36×** | Rust |
| xor | **58.4** | 36.5 | **1.60×** | Rust |
| aes-128 | **45.5** | 39.9 | **1.14×** | Rust |
| aes-192 | **50.1** | 43.9 | **1.14×** | Rust |
| aes | **50.3** | 35.2 | **1.43×** | Rust |
| sm4 | **24.3** | 13.0 | **1.87×** | Rust |
| tea | **36.5** | 27.7 | **1.32×** | Rust |
| xtea | **23.6** | 18.7 | **1.26×** | Rust |
| salsa20 | 22.6 | **36.3** | 0.62× | Go |
| blowfish | **33.4** | 20.2 | **1.65×** | Rust |
| twofish | **34.3** | 22.4 | **1.53×** | Rust |
| cast5 | **24.9** | 23.9 | **1.04×** | Rust |
| 3des | **8.4** | 8.1 | **1.04×** | Rust |
| aes-128-gcm | **61.7** | 48.2 | **1.28×** | Rust |

带压缩（Snappy）：

| 加密算法 | Rust-Tokio | Go | R/Go | 胜者 |
|---------|:----------:|:--:|:----:|:----:|
| null | 50.6 | **52.3** | 0.97× | Go |
| none | **59.7** | 48.8 | **1.22×** | Rust |
| xor | **54.7** | 46.8 | **1.17×** | Rust |
| aes-128 | **60.2** | 32.9 | **1.83×** | Rust 🏆 |
| aes-192 | **48.6** | 33.4 | **1.46×** | Rust |
| aes | **50.3** | 37.8 | **1.33×** | Rust |
| sm4 | **23.8** | 7.5 | **3.17×** | Rust 🏆 |
| tea | **40.7** | 29.8 | **1.37×** | Rust |
| xtea | **13.7** | 11.0 | **1.25×** | Rust |
| salsa20 | 25.4 | **27.5** | 0.92× | Go |
| blowfish | **29.4** | 21.9 | **1.34×** | Rust |
| twofish | **33.2** | 20.4 | **1.63×** | Rust |
| cast5 | **31.3** | 23.2 | **1.35×** | Rust |
| 3des | **12.1** | 7.9 | **1.53×** | Rust |
| aes-128-gcm | **48.4** | 38.6 | **1.25×** | Rust |

> **Linux VPS 结果：** Rust-Tokio 在 30 个加密×压缩组合中赢下 **28 个**。仅有的两个例外是 `salsa20`（Go 的 Salsa20 实现高度优化）和 `null`+压缩（无加密开销时 Go 的 Snappy 在可压缩数据上略胜）。突出倍率：**SM4+压缩 3.17×**、**AES-128+压缩 1.83×**、**XOR 1.60×**、**Blowfish 1.65×**、**Twofish 1.53×**。前提是调优内核 UDP 缓冲区（`net.core.rmem_max=4MB`）—— 默认 208 KB 缓冲区下，两端都会因静默丢包损失 ~80% 吞吐。

</details>

<details>
<summary><strong>压力测试（数据完整性）</strong></summary>

全部 10 项压力测试通过 — 在并发负载下验证**逐字节精确性**：

| 测试 | 连接数 | 负载大小 | 结果 |
|------|:-----:|:--------:|:----:|
| 单连接混合大小 | 1 | 1B…64KB | ✅ |
| 单连接 1 MB | 1 | 1 MB | ✅ |
| 多线程 10 连接 | 10 | 各 256B | ✅ |
| 多线程 50 连接 | 50 | 各 255B | ✅ |
| 多线程 100 连接 | 100 | 1B + 4KB | ✅ |
| 大数据（100 连接） | 100 | 各 64KB + 128KB | ✅ |
| 页面刷新模拟 | 80（3 波） | 512B…128KB | ✅ |
| 可压缩数据 | 1 | 压缩模式 | ✅ |
| 多端口监听 | 2 | 2 端口范围 | ✅ |
| closeWait 冒烟 | 1 | echo | ✅ |

</details>

## 📡 客户端 I/O 模式对比

`latency_p99` 示例用于测量裸 `kcp_rs::KcpStream` 的往返延迟。生产（也是唯一）的客户端架构是 **per-connection tokio task** —— 实验性的 WorkerPool 客户端模式（`--wp-client` / `--wp-workers`）已移除：额外的跨线程唤醒在实测中拖累了 P50 与吞吐量。

```bash
# 1. 在独立进程中启动 echo server（避免 CPU 竞争）
cargo run -p kcp-rs --features async --example latency_p99 -- --mode server --port 39001

# 2a. 开环模型：固定请求速率
cargo run -p kcp-rs --features async --example latency_p99 -- \
    --mode peer --addr 127.0.0.1:39001 --rps 200 --warmup 3 --duration 10

# 2b. 闭环模型：保持 N 个在途请求
cargo run -p kcp-rs --features async --example latency_p99 -- \
    --mode peer --addr 127.0.0.1:39001 --concurrency 128 --warmup 3 --duration 10
```

`--rt single|multi` 与 `--workers N` 只决定基准进程自身使用哪种 runtime（单线程可避免跨线程唤醒，多线程贴近生产形态）。完整实测数据见 [bench/LATENCY_P99_REPORT.md](bench/LATENCY_P99_REPORT.md)。

## 🔗 Go 兼容性

kcptun-rs 与 Go kcptun（kcp-go v5）**完全线上兼容**。全部 68 项端到端互通测试全部通过：

| 功能 | 状态 | 说明 |
|------|:----:|------|
| KCP 段格式 | ✅ | 24 字节小端序头部，与 kcp-go v5 一致 |
| Crypto 头部（CFB） | ✅ | `[nonce 16B][CRC32 4B][payload]` |
| AES-GCM | ✅ | `[nonce 12B][ciphertext+tag 16B]` |
| Snappy（会话级） | ✅ | 与 Go 的 `github.com/golang/snappy` 字节一致 |
| SMUX v1 & v2 | ✅ | 完整帧格式兼容 |
| FEC（10/3、4/2） | ✅ | Reed-Solomon，相同头部格式 |
| 密钥派生 | ✅ | PBKDF2-HMAC-SHA1，盐值 `b"kcp-go"` |
| QPP 混淆 | ✅ | 流级，相同置换算法 |
| 全部 15 种加密算法 | ✅ | 双向（Go→Rust、Rust→Go）|
| 全部 4 种 KCP 模式 | ✅ | normal、fast、fast2、fast3 |
| SM4（国密标准） | ✅ | tjfoc/gmsm S-box + CK 修正 |
| CAST5（RFC 2144） | ✅ | 完整实现，从 Go 移植 |

### 端到端测试结果

```text
加密算法:    15/15 种通过（Go→Rust + Rust→Go）
KCP 模式:    4/4 通过
SMUX:        2/2 版本通过
压缩:        7/7 种加密×压缩组合通过（14 项检查）
FEC:         2/2 配置通过
总计:        68 通过，0 失败，0 跳过 🎉
```

## 🏗️ 架构

### 协议栈

```text
┌──────────────────────────────────┐
│         TCP / UNIX Socket        │
├──────────────────────────────────┤
│        SMUX Stream (多路复用)     │
├──────────────────────────────────┤
│       SMUX Session (多路复用)     │
├──────────────────────────────────┤
│  Snappy 压缩 (会话级)             │  ← 与 Go 字节一致
├──────────────────────────────────┤
│  BlockCrypt / FEC / KCP (ARQ)    │
├──────────────────────────────────┤
│           UDP / TCPraw           │
└──────────────────────────────────┘
```

### 工作空间（9 个 crate）

```text
kcptun-rs/
├── kcp-rs/          — KCP ARQ 协议状态机
├── kcrypt-rs/       — 加密引擎（15 种方法，含 AES-128-GCM）
├── smux-rs/         — SMUX 流多路复用器 (v1/v2)
├── qpp-rs/          — 量子置换垫混淆
├── knet-rs/          — 异步 I/O 抽象 (tokio)
├── kpprof-rs/       — Go 兼容 pprof HTTP 服务
├── kcptun-common/   — 客户端/服务端共享辅助
├── kcptun-client/   — 客户端二进制
└── kcptun-server/   — 服务端二进制 + 压力测试
```

### 运行时设计

- **tokio**（唯一运行时）— 多线程、高并发、适合生产规模
- 业务代码仅使用 `knet::*` 抽象 — 绝不直接使用 tokio API

### 会话写管线

会话写循环（`kcptun-common::kcptun_session::write_loop`）把昂贵的阶段全部挡在 KCP 互斥锁之外：

| 步骤 | 工作内容 | KCP 锁 |
|:----:|---------|:------:|
| 1 | 排空 SMUX 发送缓冲区，编码帧与 FIN（`prepare_outbound_into_controlled`） | ❌ 未持有 |
| 2 | 回收过期/linger 超时的流，并补发其 FIN | ❌ 未持有 |
| 3 | Snappy 压缩（会话级）；大队列卸载到 `cpu_block`，超时后内联回退 | ❌ 未持有 |
| 4 | `kcp.write_all(batch)` — 唯一触碰 KCP 的调用（`update`/`flush`/重传由 `KcpStream` 自身的事件驱动 flush loop 负责） | ✅ 短暂持有 |

接收侧同理：FEC 解码在锁外完成，整个 UDP burst 只加一次锁（`process_inbound_batch`）。这使得接收循环可以在写循环准备下一批帧的同时持续向 KCP 喂数据 — 消除了高并发下的锁争用。

## 🔧 构建与运行

```bash
make build          # 调试构建（tokio）
make release        # 发布构建（LTO、strip、panic=abort）
make test           # 全部单元测试
make stress         # 数据完整性压力测试（需先构建 release）
make e2e            # Go↔Rust 互通测试（需 Go kcptun 二进制）
make clippy         # 代码检查（警告 = 错误）
make fmt            # 格式化所有 Rust 代码
make profile        # 火焰图性能分析（samply → Speedscope）
```

### 交叉编译

```bash
make release-armv7     # 树莓派 2/3、OpenWrt（二进制约 1.3M）
make release-arm64     # 树莓派 4/5、AWS Graviton
make linux             # x86_64 Linux musl（从 macOS 交叉编译）
make linux-aarch64     # ARM64 Linux musl（从 macOS 交叉编译）
make windows           # Windows x86_64 .exe（从 macOS/Linux 交叉编译）
```

所有交叉构建都使用 **tokio** 运行时和二进制默认特性集：`qpp` + `pprof` 均为默认特性，因此 pprof **并未**禁用。也正因如此，`*-full` 目标当前与基础目标构建的特性集相同。

### 从 macOS/Linux 交叉编译 Windows 二进制

使用 mingw-w64（GNU 工具链）交叉编译 `kcptun-client.exe` / `kcptun-server.exe`：

```bash
rustup target add x86_64-pc-windows-gnu   # 添加 rustup target
brew install mingw-w64                    # macOS；Debian：apt install gcc-mingw-w64-x86-64

make windows           # 发布构建 → target/x86_64-pc-windows-gnu/release/*.exe
make build-windows     # 调试构建
```

Rust 的 `x86_64-pc-windows-gnu` 标准库采用自包含链接，产物 `.exe` 不依赖 `libgcc_s_seh-1.dll` / `libwinpthread-1.dll`，直接复制到 Windows 即可运行。导入表只有系统组件（`kernel32`、`ws2_32`、`advapi32`、`bcrypt`、`ntdll` + UCRT），因此需要 Windows 10 及以上。此处不构建 MSVC 三元组（`x86_64-pc-windows-msvc`），它需要 Windows 主机或 xwin + MSVC SDK。

> Windows 运行时行为未纳入 CI：raw-TCP（`--tcp`）需要管理员权限；`--pprof` 可提供 heap/allocs，但 CPU profile 返回 501（kpprof-rs 的 CPU 采样仅支持 Unix）。

### 系统级 UDP 缓冲区调优（macOS）

macOS 默认的 UDP socket 缓冲区很小（发送/接收通常各 256 KB）。在高吞吐 KCP 负载下 —— 尤其是大窗口（`sndwnd`/`rcvwnd` ≥ 512）或高并发场景 —— 内核 UDP 接收缓冲区可能溢出，导致**静默丢包**，直接推高 P99/P999 尾部延迟并触发 KCP 重传风暴。

增大内核 socket 缓冲区上限可消除此瓶颈。这是 macOS 上对 P99/P999 延迟最有效的系统级调优：

```bash
# 将最大 socket 缓冲区提升到 8MB（默认 ~256KB）
sudo sysctl -w kern.ipc.maxsockbuf=8388608

# 将 UDP 接收缓冲区提升到 4MB（默认 ~256KB）
sudo sysctl -w net.inet.udp.recvspace=4194304
```

> **对 P99/P999 的实测效果：** 应用上述设置后，裸 KCP 层在回环上的最大可持续吞吐从 **2975 → 3802 req/s**（tokio，+28%）和 **2411 → 2921 req/s**（Go，+21%）提升，P99 延迟从 14.2ms → 10.5ms（tokio）和 20.3ms → 15.9ms（Go）下降。完整数据见 [bench/LATENCY_P99_REPORT.md](bench/LATENCY_P99_REPORT.md)。

要使更改在重启后持久化，添加到 `/etc/sysctl.conf`：

```text
kern.ipc.maxsockbuf=8388608
net.inet.udp.recvspace=4194304
```

> **Linux 等效设置：** `net.core.rmem_max`、`net.core.rmem_default`、`net.core.wmem_max`、`net.core.wmem_default` —— 设为 `4194304` 或更高。部分发行版还需调 `net.core.netdev_max_backlog`。

### 运行时环境变量

相关组件启动时会从环境读取对应的运行时调优参数：

| 变量 | 默认值 | 作用域 | 说明 |
|---|---|---|---|
| `KCP_BUSY_YIELDS` | `0`（禁用） | kcp-rs `KcpStream::read` | 自旋上限忙轮询：`read()` 在挂起等待唤醒 `Notify` 之前先 `yield_now` 自旋的次数。用于补偿 tokio 的 notify→wake→schedule→poll 调度跳变（每次唤醒 ~0.2–2ms）—— Go 运行时的 µs 级 goroutine 唤醒天然没有这个代价。 |

**`KCP_BUSY_YIELDS` 使用规则**（arm64 macOS 实测，500 RPS × 26 KB 回声；见 [bench/LATENCY_P99_REPORT.md](bench/LATENCY_P99_REPORT.md)）：

- **少连接、延迟敏感的请求发起端**：可设为 `512`，以额外 CPU 换取更低的唤醒尾延迟。
- **监听端 / 高并发服务端**：保持 `0`。自旋 reader 会与连接处理及 flush task 争抢调度资源，可能显著放大 P99。
- **吞吐优先 / 闭环负载**：保持 `0`。实测自旋会拖累闭环 req/s。因此 `bench/run_p99.sh` 只给独立的 Rust→Go 请求发起端（组合 3）设置 `512`；Rust↔Rust self 模式、Rust 服务端和闭环测试明确使用事件驱动配置。

库默认值保持 `0`，生产部署保持事件驱动语义。`KcpListener` 完全运行在调用方的 runtime 上（没有私有 runtime）；`kcptun-server` 为每个多 shard socket 配备独立的 OS 线程 + current-thread runtime，共享多线程 Tokio runtime 的 worker 数由 `--threads N` 设置（0 = 自动）。

## 🎛️ 延迟调优指南（Linux）

来自 P99 优化工作（2026-09）的实用调优结论。全部数据来自 1 vCPU Linux 虚拟机上
受控同机 A/B 测试（[docs/kcp-rs-optimization-2026-09-01.md](docs/kcp-rs-optimization-2026-09-01.md) §6–§9）。

### 1. 监听器架构（无需操作）

`KcpListener`（`kcptun-server` 使用）完全运行在**调用方的 runtime** 上：单个异步接收任务排空共享 socket，将每个 burst 按源地址分组，并就地喂给每个存活会话 —— 每个 peer 每个 burst 只有一次解密、一次 KCP 加锁、一次 flush 和一次发送系统调用。不再有 worker 数或拓扑开关（旧的 `KCPTUN_WORKER_THREADS` 已删除）；并行度来自 `kcptun-server` 的 shards（见下），共享 Tokio worker 数由 `--threads` 设置。

### 2. kcptun-server 的 `--shards N`（Linux）

每个 shard 在监听端口上绑定自己的 SO_REUSEPORT socket，不存在共享 fd 的发送争用。

- 默认（`--shards 0`）：**Linux** 上每个逻辑 CPU 一个 shard；其他平台单 shard（macOS 的 SO_REUSEPORT 不分发 UDP 流）。
- **硬上限 64**：每个 shard 绑定自己的 UDP socket；多 shard 时为每个 socket 配备独立 OS 线程 + current-thread runtime（严格 fd 亲和），单 shard 情况则作为共享 runtime 上的一个任务运行。更大的值会在启动时报错退出（以前会 `expect` panic/abort）。
- 经验法则：**shards ≈ vCPU 数**。1–2 vCPU 的机器保持 1 个 shard —— 多余的 worker 在同一个核上只增加跨核唤醒（N=2 reuseport 路径功能验证正确，但在单 vCPU 上严格更慢）。

### 3. 内核 CFS wakeup granularity —— Linux 上对 P99 影响最大的单项参数

默认的 `kernel.sched_wakeup_granularity_ns`（很多发行版为 15ms，包括 CentOS 7 / kernel 3.10）允许被唤醒的任务最长等待 15ms 才可能抢占当前任务。唤醒链的每一跳（socket 事件 → runtime → KCP task → flush task）都可能吸收这道门槛，在繁忙的核上产生偶发的 **8–12ms P999 尾延迟簇**。设为 1ms 即可消除该簇：同样的 3 轮验证里 Rust 的 p99 从 394µs 降到 **112µs**（p50/p90 为 93/91 → 19/78µs），**在每个分位上都反超 Go**（Go 天然免疫，因为 goroutine 直接在已运行的 P 上用户态重调度）。

```bash
# 临时生效
sudo sysctl -w kernel.sched_wakeup_granularity_ns=1000000

# 持久化（推荐用于延迟敏感主机）
echo 'kernel.sched_wakeup_granularity_ns=1000000' | sudo tee /etc/sysctl.d/99-kcptun-low-latency.conf
sudo sysctl --system
```

版本差异：该参数在 kernel ≤ 5.15 位于 `/proc/sys/kernel/` 路径；5.16–6.5 移到 `/sys/kernel/debug/sched/`（需挂载 debugfs）；**≥ 6.6（EEVDF 调度器）已彻底移除**（该门槛机制不存在了，直接跳过即可）。它是主机级设置、由运维持有，因此二进制自身从不修改它。

### 4. 系统级 UDP 缓冲区（Linux）

```bash
sudo sysctl -w net.core.rmem_max=4194304
sudo sysctl -w net.core.wmem_max=4194304
```

二进制已为每个 socket 申请 4MB（`knet`）；这些 sysctl 只是把内核上限抬高，让申请真正生效。

### 5. 快速清单

| 场景 | 设置 |
|:-----|:-----|
| 1–2 vCPU 主机（VPS、容器） | 1 个 shard（`--shards 1`；非 Linux 平台默认就是 1）+ `wakeup_granularity=1ms` |
| 多核主机、大量并发会话 | 默认 `--shards`（= CPU 数，Linux 上为 reuseport 组）+ `wakeup_granularity=1ms` |
| 高丢包网络 | 保持默认；FEC（`--datashard/--parityshard`）用带宽换尾延迟 |
| 吞吐基准测试 | 恢复 `wakeup_granularity` 默认值（1ms 会略微增加公平切换开销） |

## 🔬 优化历程

本项目通过火焰图驱动的性能分析，从最初的 **5.4 MB/s** 进化到超过 **108 MB/s**：

| 里程碑 | 吞吐量 | vs Go |
|:------|:------:|:-----:|
| 初始移植 | 5.4 MB/s | 0.71× |
| + 事件驱动刷新调度 | 7.1 MB/s | 0.87× |
| + 零拷贝 KCP 输出管道 | 68.8 MB/s | 1.43× |
| + ARMv8 AES 硬件加速 | ~85 MB/s | 1.67× |
| + Tokio 持久阻塞线程池 | +108% | 2.1× |
| + SMUX v2 写窗口控制 | 性能瓶颈消除 | — |
| + Snappy 卸载与阈值调优 | — | — |
| + sendmmsg/recvmmsg 批量 I/O | — | — |
| + 加密算法枚举静态分发 | vtable 消除 | — |
| + macOS UDP 缓冲区调优（sysctl）| P99 −26%，吞吐 +28% | — |
| + tokio-aware worker 队列（flush 定时器与 driver 共享 epoll） | 开放模型 p99 −17%，p999 −72~85% | — |
| + 直连 worker 拓扑（单 worker 直排 + Linux SO_REUSEPORT 组） | 闭环吞吐 +4.1~6.6%，p99 再降 −6~8% | — |
| → **最终（tokio）** | **85.6 MB/s** | **1.67×** |

<details>
<summary><b>沿途发现的关键 Bug 修复</b></summary>

| Bug | 影响 | 修复 |
|:----|:----|:-----|
| Blowfish 每块密钥调度 | 0.0 MB/s（100 倍提升） | 缓存加密器实例 |
| Twofish 每块密钥调度 | 0.4 → 4.5 MB/s（11 倍） | 自定义预计算表 |
| Snappy 中 CRC32C vs CRC32/IEEE | 数据被 Go 静默丢弃 | 改用 `snap::FrameEncoder` |
| KCP ACK 从未填充 | 无限重传 → 死锁 | 对每个收到的 Push 段排队 ACK |
| `snd_buf` 从不清理 | 窗口卡在 32 个包 | flush() 中前缓冲清理 |
| Twofish 256 位密钥 S-box | 与 Go 密文不符 | 增加第 5 层 sbox |

</details>

<details>
<summary><b>p99 延迟崩塌排查（256KB @ 高并发）</b></summary>

症状：裸 `kcp-rs` KcpStream（无隧道层）在大包高并发下崩塌 —— 256KB 回环
**RPS=300 时从 ~4ms 飙到 p50=3.2s**，而 Go 用*完全相同*的 512/512 窗口 +
Fast3 配置保持 **19ms**。单请求延迟本来就快（4.3ms）；只有当请求开始重叠时
管道才停滞。

**根因：** 每个收到的 KCP 段都会触发一次完整的 `flush_with_current()`
（kcp.input → `parse_una>0` → 遍历整个 `snd_buf` ~500 段做重传检查）。
50k+ pkt/s 下 ≈ 3000 万次 snd_buf 迭代/秒，每段成本膨胀到 ~680µs，并驱动
fast/early 重传风暴（~20K/2s）。

**有效修复（保留）：**

| 修复 | 位置 | 效果（256KB@RPS=300） |
|:----|:-----|:---------------------|
| 批量 input flush：`input_no_flush()` + `flush_if_pending()`（每个 recv 批次只做一次推迟的 flush、一次锁） | kcp.rs, conn.rs | **3183ms → 2.1ms**，100% 成功（原 87%），重传风暴归零 |
| flush loop 信任 `kcp.flush()` 返回值（clamp 1..10ms）而非强制 1ms | conn.rs | flush loop 开销降低 ~5-10× |
| P3：nodelay 窗口探测间隔 500→50ms（`IKCP_PROBE_INIT_NODELAY`） | kcp.rs | 崩塌边缘恢复 947ms → 47ms（RPS=250）|
| 同样的批量 flush 同步到 **legacy 二进制会话**（`input_no_flush` + 每个 datagram FEC 组一次 `flush_if_pending`）| kcptun-client, kcptun-server | 256KB@RPS=300：p50 12.4→10.5ms（legacy 隧道本就不崩 —— 1024 窗口 + FEC + SMUX 缓冲让它低于崩塌线）|
| fast/early 重传加 `new_segs_count > 0` 门控 —— 只在窗口能载新数据时重传；满窗口下在途段的 fastack 通常是延迟 ACK 而非丢失 | kcp.rs | RPS=300 p99 69ms→3.9ms；RPS≤450 干净（~2.3ms）|
| `write_notify` 改 `notify_one()`（存 permit）—— 原 `notify_waiters` 在 waiter 注册前到达的 notify 会丢失，负载下触发 10ms 兜底 | conn.rs | RPS=475 干净 2.3ms（原 539ms 崩塌）；RPS=500 p50 500ms+→~100ms |

**隧道对比（raw 极端负载排队非 lib 缺陷的证据）**：同一个 `kcp_rs::KcpStream`
按产品用法（默认共享 session 隧道，`copy_bidirectional` 每连接双向独立
任务）**256KB@RPS=500 只有 ~11ms、100% 成功**（Go 隧道 30.5ms）。raw benchmark
残余的 RPS=500 深排队是单任务串行 echo 在单连接 131MB/s 的最坏情况；隧道的
SMUX/TCP 层解耦了读写。macOS 无公开 `sendmmsg`/`recvmmsg`（libSystem 无符号），
批量收发需裸 syscall（未实施）。

Rust 现在以 ~2.1ms 撑住 300 RPS 的 256KB —— **比 Go 的 19ms 快 ~9 倍**。
wire 格式不变；已通过 Go↔Rust 双向互操作验证（各 500/500）。

**无效方案（已测试并回退，记录以免重试）：**

| 方案 | 结果 |
|:-----|:-----|
| 非对称窗口（rcv_wnd=2048）| 无变化 —— **证伪了 wnd=0 死锁是主因**（Go 也用 512/512）|
| `rmt_wnd==0` 时抑制重传 | 无变化 —— 崩塌期间 `rmt_wnd` 一直 >0 |
| 完全禁用 fast/early 重传 | 更差 4× —— 重传在恢复真实丢包 |
| ackOnly input flush（跳过 snd_buf 扫描）| 更差 5× —— flush 的数据恢复部分是关键 |
| 排空优先 recv 循环（+ `yield_now`）| 死锁 —— reactor 等待原本就是隐式 yield |
| listener reader 批量排空 | 更差 3.7× |

</details>

## 🧪 测试严谨性

| 测试类型 | 数量 | 验证内容 |
|:--------|:---:|---------|
| 测试 | 551 | `cargo test --workspace` — 422 单元 + 118 集成 + 11 文档测试 |
| E2E 互通 | 68 | Go↔Rust 双向兼容性 |
| 压力测试 | 10 | 大规模下逐字节数据完整性 |
| 稳定性长跑 | PASS | 远端 1 小时 × 20 客户端正式测试，六项判据全过（停摆阈值 2026-10-12 起为 150 秒） |
| 完整性猎捕 | 2708 万 / 0 | 仪器化交换逐字节校验（约 358 GB），截至 2026-10-15 零失败 |
| Clippy | `-D warnings` | 零警告强制 |
| Fmt | `cargo fmt --check` | 一致的代码格式 |

停摆说明：高丢包下 KCP 重传槽位会膨胀至与 Go 完全一致的 `RTO_MAX`（60 秒）上限——属可自愈的算法固有行为，不是缺陷
（`bugs/ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS.md`）。监听器不会仅因 `dead_link` 状态拆除会话，与 Go 行为一致。

## 📋 CLI 选项

下表中的默认值为**生效值**（部分默认值在代码中解析，`--help` 不会显示）。

### kcptun-client

| 参数 | 默认值 | 说明 |
|:-----|:--------|:-----|
| `-l` / `--localaddr` | `:12948` | 本地 TCP 监听地址 |
| `-r` / `--remoteaddr` | `vps:29900` | 远端服务端地址（IP、DNS 域名或多端口范围 `host:29900-29910`） |
| `-k` / `--key` | `it's a secrect` | 预共享密钥（也支持 `KCPTUN_KEY` 环境变量） |
| `--crypt` | `aes` | 加密算法；未知名称启动即失败。生产建议 `aes-128-gcm`（唯一带认证的算法） |
| `-m` / `--mode` | `fast` | KCP 模式：`normal`/`fast`/`fast2`/`fast3`；其他值进入 manual 模式并使用下面四个旋钮 |
| `--conn` | `1` | UDP 连接数（**1–256**，超出会被拒绝） |
| `--autoexpire` | `0`（关闭） | 会话创建 N 秒后替换池槽位（被替换会话继续服务，随后由 scavenger 回收） |
| `--scavengettl` | `600` | 退役会话的回收 TTL（秒） |
| `--mtu` | `1350` | 最大传输单元（**≥ 50**，且需容纳 2048 字节接收缓冲） |
| `--ratelimit` | `0` | 每连接限速（字节/秒，0 = 关闭） |
| `--sndwnd` | `128` | KCP 发送窗口 |
| `--rcvwnd` | `512` | KCP 接收窗口 |
| `--datashard` | `10` | FEC 数据分片 |
| `--parityshard` | `3` | FEC 校验分片 |
| `--dscp` | `0` | IP 包 DSCP（**0–63**） |
| `--nocomp` | `false` | 关闭会话级 Snappy 压缩 |
| `--acknodelay` | `false` | ACK nodelay |
| `--nodelay` / `--interval` / `--resend` / `--nc` | `0` / `50` / `0` / `0` | 手动 KCP 旋钮（`--mode` 为已知名称时不生效） |
| `--sockbuf` | `4 MiB` | Socket 缓冲区大小（字节） |
| `--smuxver` | `2` | SMUX 版本（1 或 2） |
| `--smuxbuf` | `4 MiB` | SMUX 接收缓冲 |
| `--streambuf` | `2 MiB` | SMUX 流缓冲 |
| `--framesize` | `8192` | SMUX 最大帧长 |
| `--keepalive` | `10` | SMUX keepalive 间隔（秒；0 = 关闭） |
| `--keepalivetimeout` | `30` | 入站静默超时（秒；0 = 关闭） |
| `--ackstalltimeout` | `10` | 出站未 ACK 超时（秒；0 = 关闭） |
| `--closewait` | `0` | closeWait 宽限期（秒） |
| `--snmplog` / `--snmpperiod` | 未设置 / `60` | SNMP CSV 日志路径 + 记录周期（秒） |
| `--log` | stderr | 日志文件路径（10 MiB 轮转） |
| `--quiet` | `false` | 关闭日志输出 |
| `--tcp` | `false` | 使用 raw-TCP 传输代替 UDP |
| `--pprof` / `--pprofaddr` | `false` / `127.0.0.1:6060` | pprof HTTP 端点（同一时间只允许一个 CPU profile，额外请求返回 429） |
| `--threads` | `0`（自动） | 全局异步 runtime 的 Tokio worker 线程数 |
| `--QPP` / `--QPPCount` | `false` / `61` | QPP 混淆 + pad 数量 |
| `-c` | — | JSON 配置文件路径 |
| `-v` / `--version` | — | 打印版本并退出 |

### kcptun-server

| 参数 | 默认值 | 说明 |
|:-----|:--------|:-----|
| `-l` / `--listen` | `:29900` | KCP 监听地址（UDP；支持端口范围） |
| `-t` / `--target` | `127.0.0.1:12948` | 转发目标 TCP 地址 |
| `-k` / `--key` | `it's a secrect` | 预共享密钥（也支持 `KCPTUN_KEY` 环境变量） |
| `--crypt` | `aes` | 加密算法（同客户端）；未知名称启动即失败 |
| `-m` / `--mode` | `fast` | KCP 模式 |
| `--shards` | `0`（自动：Linux 按 CPU 数，其他平台 1） | SO_REUSEPORT shard socket 数。**硬上限 64**（更大的值以前会直接 abort 进程） |
| `--peripsessionrate` | `20` | 每源 IP 每秒新建会话上限（0 = 不限） |
| `--maxsessionsperip` | `0` | 每源 IP 并发会话上限（0 = 不限；NAT 后建议保持 0） |
| `--mtu` | `1350` | MTU（**≥ 50**） |
| `--ratelimit` | `0` | 每连接限速（字节/秒，0 = 关闭） |
| `--sndwnd` / `--rcvwnd` | `1024` / `1024` | KCP 发送 / 接收窗口 |
| `--datashard` / `--parityshard` | `10` / `3` | FEC 数据 / 校验分片 |
| `--dscp` | `0` | IP 包 DSCP（**0–63**） |
| `--nocomp` | `false` | 关闭会话级 Snappy 压缩 |
| `--acknodelay` | `false` | ACK nodelay |
| `--nodelay` / `--interval` / `--resend` / `--nc` | `0` / `50` / `0` / `0` | 手动 KCP 旋钮（`--mode` 为已知名称时不生效） |
| `--sockbuf` | `4 MiB` | Socket 缓冲区大小（字节） |
| `--smuxver` | `2` | SMUX 版本（1 或 2） |
| `--smuxbuf` / `--streambuf` / `--framesize` | `4 MiB` / `2 MiB` / `8192` | SMUX 接收缓冲 / 流缓冲 / 最大帧长 |
| `--keepalive` / `--keepalivetimeout` / `--ackstalltimeout` | `10` / `30` / `10` | SMUX keepalive、入站静默、出站 ACK 停滞超时（秒；0 = 关闭） |
| `--closewait` | `30` | closeWait 宽限期（秒） |
| `--snmplog` / `--snmpperiod` | 未设置 / `60` | SNMP CSV 日志路径 + 记录周期（秒） |
| `--log` | stderr | 日志文件路径（10 MiB 轮转） |
| `--quiet` | `false` | 关闭日志输出 |
| `--tcp` | `false` | 使用 raw-TCP 传输代替 UDP |
| `--pprof` / `--pprofaddr` | `false` / `127.0.0.1:6060` | HTTP pprof（默认仅回环；同一时间一个 CPU profile，额外请求 429） |
| `--threads` | `0`（自动） | 全局异步 runtime 的 Tokio worker 线程数 |
| `--QPP` / `--QPPCount` | `false` / `61` | QPP 混淆 + pad 数量 |
| `-c` | — | JSON 配置文件路径 |
| `-v` / `--version` | — | 打印版本并退出 |

## 💡 为什么选择 Rust？

- **内存安全** — 无野指针、无缓冲区溢出、无释放后使用
- **零成本抽象** — 枚举分发消除了热路径上的虚函数表开销
- **真正并行** — `std::thread::scope` 用于批量并行加密，无 GIL 限制
- **编译期保证** — 借用检查器在数据竞争发生前就捕获它们
- **ARM 生态** — Rust 在 aarch64 上是一等公民，支持硬件 AES（`aes_armv8`）
- **小体积二进制** — 剥离后的发布版二进制约 2 MB，远小于 Go 的静态链接文件
- **交叉编译** — 一条 `make` 命令即可在 macOS 上编译 ARM Linux 二进制

## 📝 许可证

MIT — 详见 [LICENSE](LICENSE)。

本项目是 [kcptun](https://github.com/xtaci/kcptun)（作者 [xtaci](https://github.com/xtaci)）的 Rust 移植版本。  
源码： [github.com/xsean2020/kcptun-rs](https://github.com/xsean2020/kcptun-rs)

<div align="center">

**如果觉得这个项目有用或令人印象深刻，请在 GitHub 上 ⭐ 星标！**

*用 Rust 构建，由好奇心驱动，用基准测试验证。*

</div>
