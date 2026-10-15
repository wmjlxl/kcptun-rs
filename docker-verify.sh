#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# docker-verify.sh — kcptun-rs Linux 验证脚本
#
# 在 Docker 容器内运行，验证：
#   1. tokio 后端 release 二进制可正常启动
#   2. client → server → TCP echo 端到端数据转发
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PASS=0
FAIL=0
SKIP=0

ok()   { echo "✅ $1"; PASS=$((PASS+1)); }
fail() { echo "❌ $1"; FAIL=$((FAIL+1)); }
skip() { echo "⚠️  SKIP: $1"; SKIP=$((SKIP+1)); }

# ── 0. 环境信息 ──────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════════"
echo "  kcptun-rs Linux Docker 验证"
echo "  $(uname -a)"
echo "  $(cat /etc/os-release | grep PRETTY_NAME)"
echo "═══════════════════════════════════════════════════════════════"

# ── 1. 检查二进制文件存在 ───────────────────────────────────────────────────
echo ""
echo "[1/4] 检查二进制文件"
for bin in /app/kcptun-server-tokio /app/kcptun-client-tokio; do
    if [ -x "$bin" ]; then
        sz=$(ls -lh "$bin" | awk '{print $5}')
        ok "$bin ($sz)"
    else
        fail "$bin 不存在"
    fi
done

# ── 2. 检查 --help 可执行（验证动态链接无缺失） ─────────────────────────────
echo ""
echo "[2/4] --help 冒烟测试"
for bin in /app/kcptun-server-tokio /app/kcptun-client-tokio; do
    name=$(basename "$bin")
    if "$bin" --help >/dev/null 2>&1; then
        ok "$name --help"
    else
        # --help 退出码 2 也可接受（clap 行为）
        rc=$?
        if [ "$rc" -eq 2 ] || [ "$rc" -eq 0 ]; then
            ok "$name --help (exit=$rc)"
        else
            fail "$name --help (exit=$rc)"
        fi
    fi
done

# ── 3. 检查 ldd 动态链接 ──────────────────────────────────────────────────────
echo ""
echo "[3/4] ldd 动态链接检查"
for bin in /app/kcptun-server-tokio /app/kcptun-client-tokio; do
    name=$(basename "$bin")
    if ldd "$bin" >/dev/null 2>&1; then
        missing=$(ldd "$bin" 2>&1 | grep "not found" || true)
        if [ -z "$missing" ]; then
            ok "$name ldd 无缺失库"
        else
            fail "$name ldd 缺失: $missing"
        fi
    else
        ok "$name 静态链接"
    fi
done

# ── 4. 端到端数据转发测试 (tokio 后端) ───────────────────────────────────────
echo ""
echo "[4/4] 端到端数据转发 (tokio 后端)"

run_e2e() {
    local server_bin="$1"
    local client_bin="$2"
    local label="$3"
    local crypt="$4"

    local echo_port=18080
    local udp_port=29900
    local tcp_port=12948

    # 启动 TCP echo 服务器
    python3 -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', $echo_port))
s.listen(1)
s.settimeout(10)
try:
    conn, _ = s.accept()
    data = conn.recv(65536)
    conn.sendall(data)
    conn.close()
except socket.timeout:
    pass
s.close()
" &
    local echo_pid=$!
    sleep 0.3

    # 启动 kcptun-server (UDP → TCP echo)
    "$server_bin" \
        -l 127.0.0.1:$udp_port \
        -t 127.0.0.1:$echo_port \
        --key "test" \
        --crypt "$crypt" \
        --mode fast \
        --nocomp \
        --quiet &
    local srv_pid=$!
    sleep 0.5

    # 启动 kcptun-client (TCP → UDP/KCP)
    "$client_bin" \
        -l 127.0.0.1:$tcp_port \
        -r 127.0.0.1:$udp_port \
        --key "test" \
        --crypt "$crypt" \
        --mode fast \
        --nocomp \
        --quiet &
    local cli_pid=$!
    sleep 0.5

    # 通过 client 发送数据，验证 echo 回环
    local test_data="Hello kcptun-rs from Docker!"
    local recv=""
    recv=$(echo -n "$test_data" | nc -w 5 127.0.0.1 $tcp_port 2>/dev/null || true)

    # 清理进程
    kill $cli_pid  2>/dev/null || true
    kill $srv_pid  2>/dev/null || true
    kill $echo_pid 2>/dev/null || true
    wait 2>/dev/null || true

    if [ "$recv" = "$test_data" ]; then
        ok "[$label] crypt=$crypt echo 回环成功"
        return 0
    else
        fail "[$label] crypt=$crypt echo 回环失败 (sent='$test_data' recv='$recv')"
        return 1
    fi
}

# 测试多种加密方式
for crypt in null aes aes-128-gcm salsa20; do
    run_e2e /app/kcptun-server-tokio /app/kcptun-client-tokio "tokio" "$crypt"
done

# ── 汇总 ──────────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  验证结果: ✅ $PASS 通过  ❌ $FAIL 失败  ⚠️  $SKIP 跳过"
echo "═══════════════════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    exit 1
else
    echo "🎉 全部通过！kcptun-rs 在 Linux 环境下正常工作。"
    exit 0
fi
