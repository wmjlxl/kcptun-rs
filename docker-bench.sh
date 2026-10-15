#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# docker-bench.sh — kcptun-rs Linux 矩阵测试编排
#
# 在 Docker 容器内构建 Rust + Go 二进制，并运行 bench_rust_vs_go.py 矩阵测试。
#
# 用法:
#   bash docker-bench.sh [--quick] [--rust-only] ...
#   bash docker-bench.sh --rebuild-go [--quick]
#
# 环境变量:
#   KCPTUN_GO_SRC  — Go kcptun 源码目录（默认: ../kcptun）
#
# 所有其他参数透传给 bench_rust_vs_go.py。
# 示例:
#   bash docker-bench.sh --quick              # 快速测试（少量 cipher）
#   bash docker-bench.sh --rust-only          # 仅测试 Rust-tokio
#   bash docker-bench.sh --conn 5 --size 512000  # 自定义并发和载荷
#   KCPTUN_GO_SRC=/path/to/kcptun bash docker-bench.sh  # 指定 Go 源码路径
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

cd "$(dirname "$0")"

GO_SRC="${KCPTUN_GO_SRC:-$(cd "$(dirname "$0")/.." 2>/dev/null && pwd)/kcptun}"
GO_OUT_DIR="kcptun-go-linux"
IMAGE_NAME="kcptun-bench"
SUMMARY_FILE="bench_docker_results.json"

# 项目根目录（用于 Go 交叉编译输出路径）
PROJECT_ROOT="$(pwd)"

# ── 颜色 ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info()  { echo -e "${CYAN}[INFO]${NC} $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}   $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }

# ── Step 1: 交叉编译 Go Linux 二进制 ────────────────────────────────────────
info "Step 1/4: 交叉编译 Go kcptun Linux/amd64 二进制..."

if [ ! -d "$GO_SRC" ]; then
    echo "ERROR: Go 源码目录不存在: $GO_SRC"
    exit 1
fi

mkdir -p "$GO_OUT_DIR"

# 只在二进制不存在时重新编译（或强制用 --rebuild-go）
if [ "${1:-}" = "--rebuild-go" ]; then
    shift
    rm -f "$GO_OUT_DIR"/server "$GO_OUT_DIR"/client
fi

if [ ! -f "$GO_OUT_DIR/server" ] || [ ! -f "$GO_OUT_DIR/client" ]; then
    cd "$GO_SRC"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -ldflags="-s -w" \
        -o "$PROJECT_ROOT/$GO_OUT_DIR/server" ./server
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -ldflags="-s -w" \
        -o "$PROJECT_ROOT/$GO_OUT_DIR/client" ./client
    cd - > /dev/null
    ok "Go Linux 二进制编译完成"
else
    ok "Go Linux 二进制已存在，跳过编译"
fi

ls -lh "$GO_OUT_DIR/"

# ── Step 2: 构建 Docker 镜像 ────────────────────────────────────────────────
info "Step 2/4: 构建 Docker 镜像（Rust tokio + Go）..."
info "这可能需要 10-30 分钟，具体取决于网络和机器性能..."

# 检查 Docker 是否在运行
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker 未运行。请启动 Docker Desktop 后重试。"
    exit 1
fi

docker build -f Dockerfile.bench -t "$IMAGE_NAME" . 2>&1 | tail -5
ok "Docker 镜像构建完成: $IMAGE_NAME"

# ── Step 3: 运行矩阵测试 ────────────────────────────────────────────────────
info "Step 3/4: 运行矩阵测试..."
info "参数: $@"

# 容器内运行，容器退出后自动删除
# --cap-add=SYS_PTRACE 使 lsof 能读取 /proc 中的 FD 信息
docker run --rm \
    --name kcptun-bench-runner \
    --cap-add=SYS_PTRACE \
    "$IMAGE_NAME" \
    "$@" 2>&1 | tee /dev/stderr | tail -1 | grep -q "Results saved" && \
    ok "矩阵测试完成" || \
    warn "矩阵测试可能未完全成功，请检查上方输出"

# ── Step 4: 提取结果 ────────────────────────────────────────────────────────
info "Step 4/4: 提取测试结果..."

# 运行一个临时容器来提取结果
# 如果 bench_rust_vs_go.py 保存了结果到 bench_results.json，提取它
docker run --rm --name kcptun-bench-extract \
    --entrypoint cat \
    "$IMAGE_NAME" \
    /app/bench_results.json > "$SUMMARY_FILE" 2>/dev/null && \
    ok "结果已保存到: $SUMMARY_FILE" || \
    warn "未找到结果文件（可能是测试未完成）"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo -e "${GREEN}  完成！${NC}"
echo "  Docker 镜像: $IMAGE_NAME"
echo "  结果文件:    $SUMMARY_FILE"
echo "  再次运行:    bash docker-bench.sh [参数]"
echo "  快速测试:    bash docker-bench.sh --quick"
echo "═══════════════════════════════════════════════════════════════"