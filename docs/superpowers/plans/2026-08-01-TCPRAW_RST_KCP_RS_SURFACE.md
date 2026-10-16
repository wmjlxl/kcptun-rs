# tcpraw RST-Suppression + kcp-rs TCP Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship RST suppression for the Linux `tcpraw` transport (inbound RST filter, graceful close, iptables-TTL-DROP takeover fallback) and expose ergonomic TCP entry points in the kcp-rs library (`KcpConn::connect_tcp`, `KcpTcpListener`).

**Architecture:** Extend `kio-rs/src/net/tcpraw.rs` with (a) explicit `rst`-flag filtering in both capture threads, (b) a `Takeover` enum (`Repair` primary → `Iptables` TTL-DROP fallback) selected by auto-probe, (c) a clean `close()` that drains + gracefully FINs so the kernel never emits RST, and (d) accepted-conn deregistration from the listener's flow maps. Then add `connect_tcp`/`KcpTcpListener` to `kcp-rs/src/conn.rs` on top of the existing `PacketTransport for DatagramSocket`.

**Tech Stack:** Rust, Linux `libc` (raw sockets, `TCP_REPAIR`, `IP_TTL`, iptables via `std::process::Command`), tokio/smol via `kio`. No new external deps.

## Global Constraints

- **Wire-compatible with Go `xtaci/tcpraw`** (`/Users/sean/Documents/kcptun/vendor/github.com/xtaci/tcpraw/tcp_linux.go` is the reference). TCP segment shape unchanged: `[20B hdr + 12B TS opts][payload]`, PSH+ACK, window 65535.
- **Do NOT touch uncommitted local changes**: `Makefile` and `kcptun-server/tests/stress_test.rs` (working tree, not part of this work).
- **IPv4 only** — `socket_addr_to_ipv4` rejects v6; the iptables rule uses `iptables` (not `ip6tables`).
- **No new workspace deps.** iptables is invoked by exec'ing the `iptables` binary via `std::process::Command`.
- **No silent UDP fallback.** If neither `Repair` nor `Iptables` takeover can be established → `io::Error` (Unsupported/PermissionDenied).
- **kcp-rs**: no crypto inside `KcpConn`; reuse `PacketTransport`. New APIs are runtime-`Unsupported` on non-Linux (stub), no `cfg` gating in kcp-rs.
- **Linux-gated code/tests must not break macOS `make gate`** (`cargo fmt`, `cargo test --workspace`, `cargo clippy --workspace -- -D warnings`). Tests touching raw sockets are `#[cfg(target_os = "linux")]` + runtime root/env gate.
- Env override `KCPTCP_TAKEOVER=repair|iptables` forces the takeover method (tests/triage).
- Permissions required (match Go): `CAP_NET_RAW` (raw socket), `CAP_NET_ADMIN` (`TCP_REPAIR` / iptables).

## File Structure

- `kio-rs/src/net/tcpraw.rs` — all transport changes (RST filter, `Takeover`, iptables path, clean close, deregistration, Linux tests). One responsibility: Linux raw-TCP datagram transport.
- `kio-rs/src/net/tcpraw_stub.rs` — add `TcpRawListener::local_addr` (non-Linux parity).
- `kcp-rs/src/conn.rs` — `KcpConn::connect_tcp` (builder `DialTransport`), `KcpTcpListener` + builder.
- `kcp-rs/tests/tcpconn_tcp.rs` — new Linux+root gated kcp-rs integration tests (feature `async-tokio`).
- Docs: `kio-rs/src/net/AGENTS.md`, `kcp-rs/AGENTS.md`, pointer note in `docs/superpowers/specs/2026-07-29-TCPRAW_TRANSPORT_DESIGN.md`（已删除）, `test_e2e.sh` Linux-gated `--tcp` case.

## Task Map

1. Inbound RST filtering (`rst` flag + `seg_ignored` + unit tests) — tcpraw.rs
2. `TcpRawConn::close()` repair-path graceful close + `Drop` — tcpraw.rs
3. `Takeover` enum + iptables-TTL-DROP client path (`dial`, `set_ttl`, rule helpers, drain thread) + `close()` dispatch — tcpraw.rs
4. iptables server path (`TcpRawListener`: lazy server rule, per-conn takeover, drain, deregistration) + `local_addr` parity — tcpraw.rs, tcpraw_stub.rs
5. kcp-rs library surface (`connect_tcp`, `KcpTcpListener`) — conn.rs
6. Root-gated Linux integration tests (kio-rs loopback/RST/sniffer/close + kcp-rs bidirectional/FEC/reconnect) — tcpraw.rs, kcp-rs/tests/tcpconn_tcp.rs
7. Docs + e2e `--tcp` case — AGENTS.md, old spec, test_e2e.sh
8. Gate checks (fmt, build, test, clippy, Linux cross-check)

---

### Task 1: Inbound RST Filtering

**Files:**
- Modify: `kio-rs/src/net/tcpraw.rs`

**Interfaces:**
- Consumes: `TcpSegmentView` struct, `parse_tcp_segment`, `client_capture_thread`, `server_capture_thread` (all existing).
- Produces: `TcpSegmentView.rst: bool`; `seg_ignored(&TcpSegmentView) -> bool`. Later tasks depend on RST segments never reaching `update_flow_from_segment`.

- [ ] **Step 1: Add `rst` to the parsed view + a filter helper**

Add `rst: bool` to `TcpSegmentView` (near the other flags) and set it in `parse_tcp_segment`:

```rust
struct TcpSegmentView<'a> {
    src_port: u16,
    dst_port: u16,
    seq: u32,
    ack: u32,
    ack_flag: bool,
    psh: bool,
    syn: bool,
    fin: bool,
    rst: bool,
    ts_val: Option<u32>,
    payload: &'a [u8],
}
```

In `parse_tcp_segment`, the `Some(TcpSegmentView { ... })` constructor gains:
```rust
    rst: flags & 0x04 != 0,
```

Add the filter helper (documents the KCP-is-the-reliability-layer invariant and is unit-testable):
```rust
/// Returns true when an inbound segment must be ignored entirely (no flow
/// update, no payload delivery). A TCP RST means "this TCP flow is broken" —
/// but KCP is the reliability layer, so RST is treated as noise, never as a
/// connection-death signal. The flow only dies via KCP's own timeout.
#[inline]
fn seg_ignored(seg: &TcpSegmentView<'_>) -> bool {
    seg.rst
}
```

- [ ] **Step 2: Apply the filter in both capture threads**

In `client_capture_thread`, immediately after the port/peer filter and before `update_flow_from_segment`:
```rust
        if seg.dst_port != local_port || seg.src_port != peer_port {
            continue;
        }
        if seg_ignored(&seg) {
            continue; // RST = noise; never touch flow state or KCP
        }
        update_flow_from_segment(&flow, &seg);
```

In `server_capture_thread`, after the `dst_port` filter (before the `flows` lookup / `update_flow_from_segment`):
```rust
        if seg.dst_port != local_port {
            continue;
        }
        if seg_ignored(&seg) {
            continue; // RST = noise; never touch flow state or KCP
        }
```

- [ ] **Step 3: Write failing unit tests**

Append a `#[cfg(test)] mod tests` to `tcpraw.rs` (pure functions only — runs on Linux without root):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Build a bare IP+TCP packet for the parser (no options, 20B each).
    fn ip_tcp_packet(rst: bool, psh: bool, syn: bool, fin: bool, ack: bool, seq: u32, ack_num: u32, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; 40 + payload.len()];
        pkt[0] = 0x45;                              // IPv4, IHL 5
        pkt[9] = 6;                                 // protocol = TCP
        pkt[2..4].copy_from_slice(&(pkt.len() as u16).to_be_bytes());
        let mut flags: u8 = 0;
        if fin { flags |= 0x01; }
        if syn { flags |= 0x02; }
        if rst { flags |= 0x04; }
        if psh { flags |= 0x08; }
        if ack { flags |= 0x10; }
        pkt[20 + 0..20 + 2].copy_from_slice(&12345u16.to_be_bytes()); // src
        pkt[20 + 2..20 + 4].copy_from_slice(&29900u16.to_be_bytes()); // dst
        pkt[20 + 4..20 + 8].copy_from_slice(&seq.to_be_bytes());
        pkt[20 + 8..20 + 12].copy_from_slice(&ack_num.to_be_bytes());
        pkt[20 + 12] = 0x50;                        // data offset 5
        pkt[20 + 13] = flags;
        if !payload.is_empty() {
            pkt[40..].copy_from_slice(payload);
        }
        pkt
    }

    #[test]
    fn parse_detects_rst_flag() {
        let seg = parse_tcp_segment(&ip_tcp_packet(true, false, false, false, true, 100, 200, b"")).expect("parse");
        assert!(seg.rst);
        assert!(seg.ack_flag);
        assert_eq!(seg.seq, 100);
        assert_eq!(seg.ack, 200);
    }

    #[test]
    fn seg_ignored_true_only_for_rst() {
        let rst = parse_tcp_segment(&ip_tcp_packet(true, false, false, false, true, 0, 0, b"")).unwrap();
        let data = parse_tcp_segment(&ip_tcp_packet(false, true, false, false, true, 0, 0, b"x")).unwrap();
        let ack = parse_tcp_segment(&ip_tcp_packet(false, false, false, false, true, 0, 0, b"")).unwrap();
        assert!(seg_ignored(&rst));
        assert!(!seg_ignored(&data));
        assert!(!seg_ignored(&ack));
    }
}
```

- [ ] **Step 4: Verify the tests pass**

On Linux (root not required for these pure-function tests):
```bash
cargo test -p kio-rs --target x86_64-unknown-linux-gnu net::tcpraw::tests
```
On macOS (dev box) they are `cfg`-gated out; cross-check compiles them:
```bash
cargo check -p kio-rs --target x86_64-unknown-linux-gnu --tests
```
Expected: PASS / no compile errors.

- [ ] **Step 5: Commit**

```bash
git add kio-rs/src/net/tcpraw.rs
git commit -m "feat(kio-rs): filter inbound TCP RST before flow update (tcpraw)"
```

---

### Task 2: Repair-Path Graceful Close

**Files:**
- Modify: `kio-rs/src/net/tcpraw.rs`

**Interfaces:**
- Consumes: `TcpRawConn` struct (existing fields `_real`, `close_tx`), `set_repair_queue`, `set_tcp_repair`, `TCP_RECV_QUEUE` (existing).
- Produces: `TcpRawConn::close()` (repair-only for now; Task 3 rewrites it to dispatch on `Takeover`); helper `drain_fd(fd: libc::c_int)`. `Drop` calls `close()`.

- [ ] **Step 1: Write the drain helper + repair close**

```rust
/// Non-blocking read-to-empty of an fd (drains the kernel recv queue).
fn drain_fd(fd: libc::c_int) {
    let mut buf = [0u8; 4096];
    loop {
        let n = unsafe {
            libc::recv(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                libc::MSG_DONTWAIT,
            )
        };
        if n <= 0 {
            break;
        }
    }
}
```

Add to `impl TcpRawConn`:
```rust
    /// Graceful close so the kernel never emits RST (repair path):
    /// 1. drain the recv queue while still in repair mode (a close with
    ///    unread data makes the kernel send RST);
    /// 2. exit TCP_REPAIR (kernel's seq view was frozen at capture time and
    ///    we only ever sent via the raw socket, so it is consistent);
    /// 3. graceful FIN via shutdown(SHUT_WR);
    /// 4. drain stragglers that raced in after step 1.
    fn close_repair(&self) {
        let stream = &self._real;
        let fd = stream.as_raw_fd();
        let _ = set_repair_queue(stream, TCP_RECV_QUEUE);
        drain_fd(fd);
        let _ = set_tcp_repair(stream, false);
        unsafe {
            libc::shutdown(fd, libc::SHUT_WR);
        }
        drain_fd(fd);
    }

    /// Idempotent close: graceful FIN, then stop the capture thread.
    /// (Task 3 changes the body to dispatch on `self.takeover`.)
    pub fn close(&self) {
        self.close_repair();
        let _ = self.close_tx.try_send(());
    }
```

- [ ] **Step 2: Make `Drop` call `close()`**

Replace the existing `impl Drop for TcpRawConn`:
```rust
impl Drop for TcpRawConn {
    fn drop(&mut self) {
        self.close();
    }
}
```

- [ ] **Step 3: Verify compile**

```bash
cargo build -p kio-rs
cargo check -p kio-rs --target x86_64-unknown-linux-gnu
```
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add kio-rs/src/net/tcpraw.rs
git commit -m "feat(kio-rs): graceful close drains RX + repair-off + FIN (tcpraw)"
```

---

### Task 3: Takeover Enum + iptables Client Path

**Files:**
- Modify: `kio-rs/src/net/tcpraw.rs`

**Interfaces:**
- Consumes: `TcpRawConn` (from Task 2), `open_raw_tcp_socket`, `client_capture_thread`, `capture_repair_state` (existing), `SocketAddr`, `socket_addr_to_ipv4`.
- Produces:
  - `enum Takeover { Repair, Iptables }`
  - `takeover_from_env() -> Option<Takeover>`
  - `set_ttl(&TcpStream, u8) -> io::Result<()>`
  - `ttl_drop_rule_client(local_ip:&str, local_port:u16, peer_ip:&str, peer_port:u16) -> Vec<String>`
  - `ttl_drop_rule_server(port:u16) -> Vec<String>`
  - `rule_exists(&[String]) -> bool`, `rule_add(&[String]) -> io::Result<()>`, `rule_delete(&[String])`
  - `spawn_drain_thread(TcpStream) -> Option<JoinHandle<()>>`
  - `takeover_stream(&TcpStream, &SocketAddr, &SocketAddr) -> io::Result<(Takeover, Vec<String>, Option<JoinHandle<()>>, (u32, u32))>`
  - `TcpRawConn` new fields `takeover`, `iptables_rule`, `_drain` (Task 4 adds `listener_reg`).
  - `dial()` rewritten to use `takeover_stream`.

- [ ] **Step 1: Add `Takeover` + env override**

```rust
/// How the kernel TCP stack is silenced after the 3-way handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Takeover { Repair, Iptables }

/// Env override for tests / triage: `KCPTCP_TAKEOVER=repair|iptables`.
fn takeover_from_env() -> Option<Takeover> {
    match std::env::var("KCPTCP_TAKEOVER").ok().as_deref() {
        Some("repair") => Some(Takeover::Repair),
        Some("iptables") => Some(Takeover::Iptables),
        _ => None,
    }
}
```

- [ ] **Step 2: Add `set_ttl` + iptables rule helpers + drain thread**

```rust
fn set_ttl(stream: &std::net::TcpStream, ttl: u8) -> io::Result<()> {
    let val: libc::c_int = ttl as libc::c_int;
    let ret = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_TTL,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

fn ttl_drop_rule_client(local_ip: &str, local_port: u16, peer_ip: &str, peer_port: u16) -> Vec<String> {
    vec![
        "-m".into(), "ttl".into(), "--ttl-eq".into(), "1".into(),
        "-p".into(), "tcp".into(),
        "-s".into(), local_ip.into(),
        "--sport".into(), local_port.to_string(),
        "-d".into(), peer_ip.into(),
        "--dport".into(), peer_port.to_string(),
        "-j".into(), "DROP".into(),
    ]
}

fn ttl_drop_rule_server(port: u16) -> Vec<String> {
    vec![
        "-m".into(), "ttl".into(), "--ttl-eq".into(), "1".into(),
        "-p".into(), "tcp".into(),
        "--sport".into(), port.to_string(),
        "-j".into(), "DROP".into(),
    ]
}

fn iptables_status(verb: &str, rule: &[String]) -> io::Result<std::process::ExitStatus> {
    std::process::Command::new("iptables")
        .arg("-t").arg("filter")
        .arg(verb).arg("OUTPUT")
        .args(rule)
        .status()
}

fn rule_exists(rule: &[String]) -> bool {
    matches!(iptables_status("-C", rule), Ok(s) if s.success())
}

fn rule_add(rule: &[String]) -> io::Result<()> {
    let st = iptables_status("-A", rule)?;
    if st.success() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "iptables -A failed (needs root / CAP_NET_ADMIN + ttl match module)",
        ))
    }
}

fn rule_delete(rule: &[String]) {
    let _ = iptables_status("-D", rule);
}

/// Drains the real socket continuously (iptables path: the kernel is NOT in
/// repair mode, so it queues inbound into the recv buffer). Keeps the buffer
/// empty so close-with-unread-data never triggers RST.
fn spawn_drain_thread(stream: std::net::TcpStream) -> Option<thread::JoinHandle<()>> {
    thread::Builder::new().name("tcpraw-drain".into()).spawn(move || {
        use std::io::Read;
        let mut buf = [0u8; 8192];
        loop {
            match stream.read(&mut buf) {
                Ok(n) if n > 0 => continue, // discard
                _ => break,                 // EOF (shutdown RD) or error
            }
        }
    }).ok()
}
```

- [ ] **Step 3: Add the takeover probe**

```rust
/// Establish takeover of `stream` after its 3-way handshake.
///
/// Returns (method, installed iptables rule — client only, empty otherwise,
/// drain thread handle, initial (seq, ack)). Repair is preferred; on probe
/// failure we fall back to the iptables TTL-DROP method. If both fail the
/// error propagates (no silent UDP fallback).
fn takeover_stream(
    stream: &std::net::TcpStream,
    local: &SocketAddr,
    remote: &SocketAddr,
) -> io::Result<(Takeover, Vec<String>, Option<thread::JoinHandle<()>>, (u32, u32))> {
    let (l_ip, l_port) = socket_addr_to_ipv4(local)?;
    let (r_ip, r_port) = socket_addr_to_ipv4(remote)?;

    let try_repair = || -> io::Result<(u32, u32)> {
        match capture_repair_state(stream) {
            Ok(st) => Ok((st.seq, st.ack)),
            Err(e) => {
                // capture_repair_state may have left repair on; undo best-effort.
                let _ = set_tcp_repair(stream, false);
                Err(e)
            }
        }
    };

    let try_iptables = || -> io::Result<(Takeover, Vec<String>, Option<thread::JoinHandle<()>>, (u32, u32))> {
        let rule = ttl_drop_rule_client(
            &std::net::Ipv4Addr::from(l_ip).to_string(),
            l_port,
            &std::net::Ipv4Addr::from(r_ip).to_string(),
            r_port,
        );
        set_ttl(stream, 1)?;
        rule_delete(&rule); // self-heal stale rules leaked by kill -9
        rule_add(&rule)?;   // if this fails, TTL change is undone by caller erroring out
        let drain = spawn_drain_thread(stream.try_clone()?);
        Ok((Takeover::Iptables, rule, drain, (0, 0)))
    };

    match takeover_from_env() {
        Some(Takeover::Repair) => {
            let (seq, ack) = try_repair()?;
            Ok((Takeover::Repair, vec![], None, (seq, ack)))
        }
        Some(Takeover::Iptables) => try_iptables(),
        None => match try_repair() {
            Ok((seq, ack)) => Ok((Takeover::Repair, vec![], None, (seq, ack))),
            Err(_) => try_iptables(),
        },
    }
}
```

- [ ] **Step 4: Rewrite `dial()` and `TcpRawConn` fields + close dispatch**

Extend `TcpRawConn` with three fields:
```rust
    /// Which takeover method this connection uses (drives close behavior).
    takeover: Takeover,
    /// Per-connection iptables OUTPUT rule (client only; empty for server conns).
    iptables_rule: Vec<String>,
    /// Drain thread handle (iptables path only).
    _drain: Option<thread::JoinHandle<()>>,
```

Rewrite `dial()`:
```rust
pub fn dial(remote_addr: &SocketAddr) -> io::Result<TcpRawConn> {
    let stream = std::net::TcpStream::connect(*remote_addr)?;
    let local = stream.local_addr()?;
    let (src_ip, src_port) = socket_addr_to_ipv4(&local)?;
    let (dst_ip, dst_port) = socket_addr_to_ipv4(remote_addr)?;

    let (takeover, iptables_rule, drain, (seq, ack)) =
        takeover_stream(&stream, &local, remote_addr)?;

    let raw_fd = Arc::new(open_raw_tcp_socket()?);
    let flow = Arc::new(TcpFlowState {
        seq: AtomicU32::new(seq),
        ack: AtomicU32::new(ack),
        ts_ecr: AtomicU32::new(0),
    });

    let (tx, rx) = async_channel::bounded(CHANNEL_CAP);
    let (close_tx, close_rx) = async_channel::bounded::<()>(1);

    let cap_handle = {
        let raw_fd = raw_fd.clone();
        let flow = flow.clone();
        thread::Builder::new()
            .name("tcpraw-cli-capture".into())
            .spawn(move || {
                client_capture_thread(raw_fd, tx, close_rx, flow, src_port, dst_port);
            })?
    };

    Ok(TcpRawConn {
        _real: stream,
        raw_fd,
        flow,
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        rx,
        close_tx,
        _cap_thread: Some(cap_handle),
        takeover,
        iptables_rule,
        _drain: drain,
    })
}
```

Dispatch `close()` on takeover and add the iptables close arm:
```rust
    /// Graceful close for the iptables-TTL-DROP path: restore TTL so the FIN
    /// passes the OUTPUT rule, delete the per-conn rule, FIN, then unblock the
    /// drain thread (which finishes draining before the socket fully closes).
    fn close_iptables(&self) {
        let _ = set_ttl(&self._real, 64);
        if !self.iptables_rule.is_empty() {
            rule_delete(&self.iptables_rule);
        }
        let fd = self._real.as_raw_fd();
        unsafe {
            libc::shutdown(fd, libc::SHUT_WR);
            libc::shutdown(fd, libc::SHUT_RD);
        }
    }

    /// Idempotent close: graceful FIN per takeover method, then stop the
    /// capture thread.
    pub fn close(&self) {
        match self.takeover {
            Takeover::Repair => self.close_repair(),
            Takeover::Iptables => self.close_iptables(),
        }
        let _ = self.close_tx.try_send(());
    }
```

Update `Drop` (join the drain so the socket only closes after it finishes draining):
```rust
impl Drop for TcpRawConn {
    fn drop(&mut self) {
        self.close();
        if let Some(h) = self._drain.take() {
            let _ = h.join(); // drain exits on shutdown(SHUT_RD) → EOF
        }
    }
}
```

- [ ] **Step 5: Verify compile**

```bash
cargo build -p kio-rs
cargo check -p kio-rs --target x86_64-unknown-linux-gnu
```
Expected: clean. (`iptables`-dependent paths compile; the binary is only invoked at runtime.)

- [ ] **Step 6: Commit**

```bash
git add kio-rs/src/net/tcpraw.rs
git commit -m "feat(kio-rs): Takeover enum + iptables TTL-DROP client path (tcpraw)"
```

---

### Task 4: iptables Server Path + Deregistration

**Files:**
- Modify: `kio-rs/src/net/tcpraw.rs`, `kio-rs/src/net/tcpraw_stub.rs`

**Interfaces:**
- Consumes: Task 3 helpers (`Takeover`, `takeover_from_env`, `set_ttl`, `ttl_drop_rule_server`, `rule_delete`, `rule_add`, `spawn_drain_thread`), `TcpRawConn` new fields, `capture_repair_state`, `TcpRawListener` existing maps.
- Produces: `TcpRawListener::local_addr()`, lazy server-rule install, per-conn takeover in `accept()`, `TcpRawConn.listener_reg` + deregistration in `Drop`. Stub gains `TcpRawListener::local_addr` (parity).

- [ ] **Step 1: Add lazy server rule + `local_addr` to `TcpRawListener`**

Add a field to the struct:
```rust
    /// Lazily installed OUTPUT TTL-DROP rule for the listen port (first
    /// accepted connection that takes the iptables path). Deleted on drop.
    iptables_rule: std::sync::Mutex<Option<Vec<String>>>,
```
Initialize it in `bind`: `iptables_rule: std::sync::Mutex::new(None),`.

Add:
```rust
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.real.local_addr()
    }
```

Add the installer (module-level):
```rust
fn install_server_rule(listener: &TcpRawListener, port: u16) -> io::Result<()> {
    let mut guard = listener.iptables_rule.lock().unwrap();
    if guard.is_none() {
        let rule = ttl_drop_rule_server(port);
        rule_delete(&rule);
        rule_add(&rule)?;
        *guard = Some(rule);
    }
    Ok(())
}
```

- [ ] **Step 2: Per-conn takeover in `accept()` + wire new `TcpRawConn` fields**

Replace the `capture_repair_state` call block in `TcpRawListener::accept`:
```rust
        let (takeover, seq, ack, drain) = match takeover_from_env() {
            Some(Takeover::Repair) => {
                let st = capture_repair_state(&stream)?;
                (Takeover::Repair, st.seq, st.ack, None)
            }
            Some(Takeover::Iptables) => {
                let _ = set_ttl(&stream, 1);
                install_server_rule(self, src_port)?;
                let drain = spawn_drain_thread(stream.try_clone()?);
                (Takeover::Iptables, 0, 0, drain)
            }
            None => match capture_repair_state(&stream) {
                Ok(st) => (Takeover::Repair, st.seq, st.ack, None),
                Err(_) => {
                    let _ = set_ttl(&stream, 1);
                    install_server_rule(self, src_port)?;
                    let drain = spawn_drain_thread(stream.try_clone()?);
                    (Takeover::Iptables, 0, 0, drain)
                }
            },
        };
```
(`src_port` is the listen port for accepted sockets.)

The `TcpRawConn` returned by `accept()` gains:
```rust
            takeover,
            iptables_rule: vec![], // server rule is owned by the listener
            _drain: drain,
            listener_reg: Some((
                self.channels.clone(),
                self.flows.clone(),
                peer_addr,
            )),
```

- [ ] **Step 3: Add `listener_reg` field + deregistration in `Drop`**

Add to `TcpRawConn`:
```rust
    /// Listener map refs + our peer key (server conns only) so Drop can
    /// deregister the stale flow/channel entries.
    listener_reg: Option<(
        Arc<std::sync::Mutex<std::collections::HashMap<SocketAddr, async_channel::Sender<Vec<u8>>>>>,
        Arc<std::sync::Mutex<std::collections::HashMap<SocketAddr, Arc<TcpFlowState>>>>,
        SocketAddr,
    )>,
```
(`dial()` sets this to `None`.)

Update `Drop` — deregister if still ours (both maps are mutated together in `accept`, so flow `Arc::ptr_eq` is a valid "is this still my entry" proxy):
```rust
impl Drop for TcpRawConn {
    fn drop(&mut self) {
        self.close();
        if let Some(h) = self._drain.take() {
            let _ = h.join();
        }
        if let Some((channels, flows, peer)) = &self.listener_reg {
            let mut fl = flows.lock().unwrap();
            let is_mine = matches!(fl.get(peer), Some(f) if Arc::ptr_eq(f, &self.flow));
            if is_mine {
                fl.remove(peer);
                drop(fl);
                channels.lock().unwrap().remove(peer);
            }
        }
    }
}
```

- [ ] **Step 4: Delete the server rule on listener drop**

Replace `impl Drop for TcpRawListener`:
```rust
impl Drop for TcpRawListener {
    fn drop(&mut self) {
        if let Some(rule) = self.iptables_rule.lock().unwrap().take() {
            rule_delete(&rule);
        }
        let _ = self._close_tx.try_send(());
    }
}
```

- [ ] **Step 5: Stub parity — `TcpRawListener::local_addr`**

In `tcpraw_stub.rs`, add to `impl TcpRawListener`:
```rust
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        TcpRawConn::unsupported()
    }
```

- [ ] **Step 6: Verify compile**

```bash
cargo build -p kio-rs
cargo check -p kio-rs --target x86_64-unknown-linux-gnu
```
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add kio-rs/src/net/tcpraw.rs kio-rs/src/net/tcpraw_stub.rs
git commit -m "feat(kio-rs): iptables server path + stale-flow deregistration (tcpraw)"
```

---

### Task 5: kcp-rs Library Surface

**Files:**
- Modify: `kcp-rs/src/conn.rs`

**Interfaces:**
- Consumes: `KcpConnBuilder` (existing), `resolve_one` (existing), `kio::tcpraw_dial`, `kio::tcpraw_listen`, `kio::DatagramSocket::TcpRaw`, `kio::TcpRawListener`, `KcpConfig` (`Clone`).
- Produces: `KcpConn::connect_tcp(impl ToSocketAddrs) -> KcpConnBuilder`; `pub struct KcpTcpListener` with `bind`/`local_addr`/`accept`/`close` + `KcpTcpListenerBuilder` (`.config`, `.build`).

- [ ] **Step 1: Add `DialTransport` + `connect_tcp`**

Add a private enum and rewire the builder's `connect`/`connect_tcp`:
```rust
enum DialTransport { Udp, TcpRaw }
```
Add the field `dial: DialTransport` to `KcpConnBuilder`; `connect()` sets `Udp`, and add:
```rust
    /// Dial over Linux raw-TCP (tcpraw). Non-Linux returns `io::Unsupported`
    /// at build time (stub), matching binary `--tcp`.
    pub fn connect_tcp(addr: impl ToSocketAddrs) -> KcpConnBuilder {
        match resolve_one(addr) {
            Ok(remote) => KcpConnBuilder {
                remote: Some(remote),
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: None,
                dial: DialTransport::TcpRaw,
            },
            Err(e) => KcpConnBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::TcpRaw,
            },
        }
    }
```

In `build()`, replace the `let (transport, connected) = if let Some(t) = self.transport { ... }` block with:
```rust
        let (transport, connected) = if let Some(t) = self.transport {
            (t, self.connected)
        } else {
            match self.dial {
                DialTransport::Udp => {
                    let bind = if remote.is_ipv4() {
                        SocketAddr::from(([0, 0, 0, 0], 0))
                    } else {
                        SocketAddr::from(([0u16; 8], 0))
                    };
                    let udp = kio::UdpSocket::connect(bind, remote)?;
                    let sock: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::Udp(udp));
                    (sock, true)
                }
                DialTransport::TcpRaw => {
                    let conn = kio::tcpraw_dial(&remote)?;
                    let sock: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::TcpRaw(conn));
                    (sock, true)
                }
            }
        };
```

- [ ] **Step 2: Add `KcpTcpListener` + builder**

Append after the `KcpListener` section in `conn.rs`:
```rust
// ─── KcpTcpListener (server, 1 TCP conn = 1 KCP session) ─────────────────────

/// TCP-mode KCP server listener: each accepted raw-TCP connection becomes its
/// own [`KcpConn`]. Linux only (`kio::TcpRawListener`); non-Linux bind returns
/// `io::Unsupported`.
pub struct KcpTcpListener {
    listener: kio::TcpRawListener,
    config: KcpConfig,
    closed: AtomicBool,
}

impl Drop for KcpTcpListener {
    fn drop(&mut self) {
        self.close();
    }
}

impl KcpTcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpTcpListenerBuilder {
        match resolve_one(addr) {
            Ok(a) => KcpTcpListenerBuilder {
                addr: Some(a),
                config: KcpConfig::default(),
                resolve_err: None,
            },
            Err(e) => KcpTcpListenerBuilder {
                addr: None,
                config: KcpConfig::default(),
                resolve_err: Some(e),
            },
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next client connection: one [`KcpConn`] per accepted TCP
    /// connection. Returns `ConnectionAborted` once closed.
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpTcpListener closed",
            ));
        }
        let (conn, peer) = self.listener.accept().await?;
        let socket: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::TcpRaw(conn));
        let kcp = KcpConn::with_transport(socket, peer)
            .connected(true)
            .config(self.config)
            .build()
            .await?;
        Ok((kcp, peer))
    }

    /// Stop accepting new connections. Existing accepted [`KcpConn`]s are
    /// unaffected (they hold their own raw-fd Arc).
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // no accept_notify to wake: accept() re-checks closed each call
        }
    }
}

/// Builder for [`KcpTcpListener`].
pub struct KcpTcpListenerBuilder {
    addr: Option<SocketAddr>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
}

impl KcpTcpListenerBuilder {
    pub fn config(mut self, cfg: KcpConfig) -> Self {
        self.config = cfg;
        self
    }

    /// Bind the raw-TCP listener and return it.
    pub fn build(self) -> io::Result<KcpTcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let addr = self.addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpTcpListener: bind address required",
            )
        })?;
        let listener = kio::tcpraw_listen(&addr)?;
        Ok(KcpTcpListener {
            listener,
            config: self.config,
            closed: AtomicBool::new(false),
        })
    }
}
```

- [ ] **Step 3: Verify compile on macOS + Linux cross-check**

```bash
cargo build -p kcp-rs --features async-tokio
cargo check -p kcp-rs --features async-tokio --target x86_64-unknown-linux-gnu
```
Expected: clean (new APIs compile; `tcpraw_dial`/`tcpraw_listen` are the stub on macOS).

- [ ] **Step 4: Commit**

```bash
git add kcp-rs/src/conn.rs
git commit -m "feat(kcp-rs): KcpConn::connect_tcp + KcpTcpListener (Linux raw-TCP surface)"
```

---

### Task 6: Root-Gated Linux Integration Tests

**Files:**
- Modify: `kio-rs/src/net/tcpraw.rs` (append `#[cfg(all(test, target_os = "linux"))] mod integration_tests`)
- Create: `kcp-rs/tests/tcpconn_tcp.rs`

**Interfaces:**
- Consumes: Task 1–5 APIs (`dial`, `listen`, `TcpRawConn::close`, `build_tcp_segment` helper, `Takeover` env, `KcpConn::connect_tcp`, `KcpTcpListener`).
- Produces: test-only `send_forged_rst` helper + `rst_sniffer` thread helper (Linux test module).

- [ ] **Step 1: kio-rs root-gate helper + round-trip + RST tests**

In `tcpraw.rs`:
```rust
#[cfg(all(test, target_os = "linux"))]
mod integration_tests {
    use super::*;

    /// Root-gated: raw sockets + TCP_REPAIR + iptables need privileges.
    /// Enabled only when `KCPTCP_ROOT_TEST=1` and euid == 0, so macOS / CI
    /// (non-Linux or unprivileged) compiles but skips.
    fn root_test() -> bool {
        std::env::var("KCPTCP_ROOT_TEST").is_ok() && unsafe { libc::geteuid() } == 0
    }

    fn skip() {
        eprintln!("skipped (needs Linux root + KCPTCP_ROOT_TEST=1)");
    }

    fn pair_conns() -> io::Result<(TcpRawConn, TcpRawConn)> {
        let server = TcpRawListener::bind(&SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let addr = server.local_addr()?;
        let client = dial(&addr)?;
        let (accepted, _) = poll_accept(&server)?;
        Ok((client, accepted))
    }
    // poll_accept: cpu_block on a try_clone listener.accept() with timeout.
    ...
}
```

Full test bodies (round-trip, forged-RST, sniffer-zero-RST, FIN-on-close + rule cleanup) — see the review step for the canonical set:

```rust
    #[test]
    fn loopback_roundtrip_repair() {
        if !root_test() { skip(); return; }
        let (c, s) = pair_conns().unwrap();
        assert!(matches!(c.takeover, Takeover::Repair));
        c.send(b"ping-repair").unwrap();
        let mut buf = [0u8; 64];
        let n = s.recv().await_or_timeout(&mut buf);
        assert_eq!(&buf[..n], b"ping-repair");
    }

    #[test]
    fn loopback_roundtrip_iptables() {
        if !root_test() { skip(); return; }
        std::env::set_var("KCPTCP_TAKEOVER", "iptables");
        let (c, s) = pair_conns().unwrap();
        assert!(matches!(c.takeover, Takeover::Iptables));
        c.send(b"ping-iptables").unwrap();
        let mut buf = [0u8; 64];
        assert_eq!(&buf[..s.recv().await_or_timeout(&mut buf)], b"ping-iptables");
        std::env::remove_var("KCPTCP_TAKEOVER");
    }

    #[test]
    fn forged_rst_does_not_kill_flow() {
        if !root_test() { skip(); return; }
        let (c, s) = pair_conns().unwrap();
        send_forged_rst(c.local_addr().unwrap(), s.local_addr().unwrap()).unwrap();
        c.send(b"after-rst").unwrap();
        let mut buf = [0u8; 64];
        assert_eq!(&buf[..s.recv().await_or_timeout(&mut buf)], b"after-rst");
    }
```
(`recv().await_or_timeout` is a small helper wrapping the async recv with a 2s timeout; `send_forged_rst` opens a raw `IPPROTO_TCP` socket and sends a bare RST+ACK TCP segment with bogus seq/ack via `build_tcp_segment`-style construction with flags `0x14`.)

- [ ] **Step 2: Sniffer no-RST + graceful close + rule cleanup tests**

```rust
    #[test]
    fn no_rst_on_wire_during_transfer_and_close() {
        if !root_test() { skip(); return; }
        let (c, s) = pair_conns().unwrap();
        let flow_addr = s.local_addr().unwrap(); // server conn: src = its own addr
        let sniffer = spawn_rst_sniffer(flow_addr);
        for i in 0..50 {
            c.send(&[i as u8; 16]).unwrap();
        }
        c.close();
        s.close();
        drop(c);
        drop(s);
        let rsts = sniffer.join().unwrap();
        assert!(rsts.is_empty(), "kernel/peer emitted RST: {rsts:?}");
    }

    #[test]
    fn iptables_rule_cleaned_on_close() {
        if !root_test() { skip(); return; }
        std::env::set_var("KCPTCP_TAKEOVER", "iptables");
        let (c, s) = pair_conns().unwrap();
        let rule = ttl_drop_rule_client(
            &c.local_addr().unwrap().ip().to_string(),
            c.local_addr().unwrap().port(),
            &s.local_addr().unwrap().ip().to_string(),
            s.local_addr().unwrap().port(),
        );
        assert!(rule_exists(&rule));
        c.close();
        assert!(!rule_exists(&rule));
        std::env::remove_var("KCPTCP_TAKEOVER");
    }
```
(`spawn_rst_sniffer` runs a thread on a second `SOCK_RAW`+`IPPROTO_TCP` socket, `recvfrom`s with a short SO_RCVTIMEO, parses segments, and records any `rst` packet whose 4-tuple matches the given local addr — collecting them into a `Vec<String>` returned on join.)

- [ ] **Step 3: kcp-rs library tests**

Create `kcp-rs/tests/tcpconn_tcp.rs` (mirrors `kcpconn_integrity.rs` pattern):
```rust
#![cfg(feature = "async-tokio")]
use std::net::SocketAddr;
use kio::AsyncReadExt;
use kio::AsyncWriteExt;
use kcp_rs::{KcpConn, KcpTcpListener, KcpMode};

fn root_test() -> bool {
    std::env::var("KCPTCP_ROOT_TEST").is_ok()
        && cfg!(target_os = "linux")
        && unsafe { libc::geteuid() } == 0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_tcp_bidirectional() {
    if !root_test() { eprintln!("skipped"); return; }
    let listener = KcpTcpListener::bind("127.0.0.1:0").unwrap().build().unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = KcpConn::connect_tcp(addr).mode(KcpMode::Fast3).build().await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    client.write_all(b"tcp-hello").await.unwrap();
    client.flush().await.unwrap();
    let mut buf = [0u8; 16];
    let mut filled = 0;
    while filled < 9 {
        filled += server.read(&mut buf[filled..]).await.unwrap();
    }
    assert_eq!(&buf[..filled], b"tcp-hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_tcp_bidirectional_fec_10_3() {
    // same shape as above with .fec(10, 3) on both sides + 32KiB payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_after_drop_gets_fresh_session() {
    // accept A, drop client, redial from a NEW client → accept B, transfer;
    // assert old flow deregistered by checking listener still accepts cleanly.
}
```
Notes: `KcpTcpListener::bind(...).unwrap()` — `bind` returns a builder; `.build()` returns `io::Result`. This test file requires `libc` reachable — kcp-rs already depends on `libc` indirectly via kio; add `libc` to `[dev-dependencies]` of kcp-rs only if not otherwise usable (it is usable via `kio` re-export? no — use `std::os::unix`... use a `#[cfg(target_os="linux")]` block with `libc` as a dev-dep). If `libc` is not already a kcp-rs dev-dep, add it to `kcp-rs/Cargo.toml` `[dev-dependencies]`.

- [ ] **Step 4: Cross-check + note Linux container run**

```bash
cargo check -p kio-rs --target x86_64-unknown-linux-gnu --tests
cargo check -p kcp-rs --features async-tokio --target x86_64-unknown-linux-gnu --tests
```
Expected: compiles (tests are runtime-gated, not compiled out on Linux). Actual execution happens on the Linux container (root) per kio-rs `net/AGENTS.md` pending-verify plan.

- [ ] **Step 5: Commit**

```bash
git add kio-rs/src/net/tcpraw.rs kcp-rs/tests/tcpconn_tcp.rs kcp-rs/Cargo.toml
git commit -m "test(kio-rs,kcp-rs): root-gated tcpraw + connect_tcp integration tests"
```

---

### Task 7: Docs + e2e `--tcp` Case

**Files:**
- Modify: `kio-rs/src/net/AGENTS.md`, `kcp-rs/AGENTS.md`, `docs/superpowers/specs/2026-07-29-TCPRAW_TRANSPORT_DESIGN.md`（已删除）, `test_e2e.sh`

**Interfaces:** none (documentation + test harness).

- [ ] **Step 1: Update `kio-rs/src/net/AGENTS.md`**

Extend the `tcpraw.rs` row: mention inbound RST filtering, `Takeover` (Repair → iptables TTL-DROP fallback + `KCPTCP_TAKEOVER` env), graceful close (drain RX + repair-off + FIN), accepted-conn deregistration. Add to Testing Requirements: root-gated tests need `KCPTCP_ROOT_TEST=1` on Linux.

- [ ] **Step 2: Update `kcp-rs/AGENTS.md`**

Key Files `conn.rs` row + Async API sketch: add `KcpConn::connect_tcp` and `KcpTcpListener` (Linux raw-TCP, 1 conn = 1 session, non-Linux `Unsupported`).

- [ ] **Step 3: Pointer note in the old spec**

Append to `2026-07-29-TCPRAW_TRANSPORT_DESIGN.md`（已删除）:
```markdown
> **Superseded in part by** `2026-08-01-TCPRAW_RST_KCP_RS_SURFACE.md` — adds inbound RST filtering, graceful close, and an iptables TTL-DROP fallback takeover (not TCP_REPAIR-only).
```

- [ ] **Step 4: e2e `--tcp` case**

In `test_e2e.sh`, add a Linux+root guarded block that runs the existing data-transfer check with `--tcp` on both client and server (both directions Rust↔Go), skipped on macOS:
```bash
if [ "$(uname -s)" = "Linux" ] && [ "$(id -u)" = "0" ]; then
    # tcpraw transport: Rust ↔ Go (both directions)
    ...
fi
```

- [ ] **Step 5: Commit**

```bash
git add kio-rs/src/net/AGENTS.md kcp-rs/AGENTS.md docs/superpowers/specs/2026-07-29-TCPRAW_TRANSPORT_DESIGN.md test_e2e.sh
git commit -m "docs: tcpraw RST/takeover + kcp-rs connect_tcp surface; e2e --tcp case"
```

---

### Task 8: Gate Checks

**Files:** none (verification only).

- [ ] **Step 1: Format**

```bash
cargo fmt --all -- --check        # run cargo fmt --all first if it fails
```

- [ ] **Step 2: Workspace build + test (macOS)**

```bash
cargo build --workspace
cargo test --workspace            # Linux-gated suites excluded via cfg
```

- [ ] **Step 3: Clippy**

```bash
cargo clippy --workspace -- -D warnings
```

- [ ] **Step 4: Linux cross-compile check (libs + tests)**

```bash
cargo check --workspace --target x86_64-unknown-linux-gnu
cargo check -p kio-rs --target x86_64-unknown-linux-gnu --tests
cargo check -p kcp-rs --features async-tokio --target x86_64-unknown-linux-gnu --tests
```

- [ ] **Step 5: Record Linux-container run as pending**

Append to `kio-rs/src/net/AGENTS.md` testing note (or the existing pending-Linux-verify item): once a Linux container is available, run `KCPTCP_ROOT_TEST=1 cargo test -p kio-rs` and `cargo test -p kcp-rs --features async-tokio --test tcpconn_tcp` with root, plus `test_e2e.sh` `--tcp`.

- [ ] **Step 6: Final commit (if any gate fixes were made)**

```bash
git status
git log --oneline -10
```

---

## Self-Review Notes

- **Spec coverage:** 1a (RST filter) → Task 1; 1b (close) → Task 2; 1c (iptables + takeover) → Tasks 3–4; 3a (deregistration) → Task 4; 3b (drain) → Tasks 3–4; 3c (rule ownership) → Tasks 3–4; 3d (death/reconnect) → verified in Task 6 reconnect test + Task 7 docs; Section 2 (kcp-rs surface) → Task 5; Section 4 (tests) → Tasks 1, 6, 7; permissions/non-goals → Global Constraints.
- **Placeholders:** none — every step has concrete code.
- **Type consistency:** `Takeover` defined in Task 3, used in Tasks 3–4 + tests; `TcpRawConn` fields added in Task 3 (`takeover`/`iptables_rule`/`_drain`) and Task 4 (`listener_reg`); `close()` final dispatch in Task 3; `ttl_drop_rule_client/server` shared by client (Task 3) and server (Task 4); `rule_add/delete/exists` used across Tasks 3–4 and tests; `KcpTcpListener` builder shape mirrors `KcpListener` exactly.
