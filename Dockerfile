# syntax=docker/dockerfile:1
# rpcrouter 多阶段构建：rust builder → debian:bookworm-slim 运行镜像。
# 源码编译发生在 builder 阶段；运行阶段仅保留静态二进制，镜像尽量精简。

# ---------------------------------------------------------------------------
# 阶段 1：编译
# 使用 rust:1.97（与 Cargo.toml 的 rust-version=1.97 对齐），编译 release 二进制。
# ---------------------------------------------------------------------------
FROM rust:1.97 AS builder

WORKDIR /build

# 先复制清单并预拉取依赖，最大化利用 Docker 层缓存。
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY fixtures ./fixtures

# release profile 已开启 thin LTO + 单 codegen unit，链接略慢但镜像内产物最优。
RUN cargo build --release --bin rpcrouter

# ---------------------------------------------------------------------------
# 阶段 2：运行镜像
# 仅拷贝二进制与默认配置；debian:bookworm-slim 足够满足 rustls(TLS) 运行需求。
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

# 以非 root 运行，降低被攻破后的影响面。
RUN groupadd --gid 10001 rpcrouter \
    && useradd --uid 10001 --gid 10001 --home-dir /app --no-create-home rpcrouter

WORKDIR /app

COPY --from=builder /build/target/release/rpcrouter /usr/local/bin/rpcrouter
# 运行时仍可被环境变量/挂载配置覆写；默认配置启用全部 8 条链。
COPY config.toml /app/config.toml
COPY cluster.toml /app/cluster.toml

# chainlist 磁盘缓存跨重启复用；运行时由卷挂载到 /app/data。
RUN mkdir -p /app/data && chown -R rpcrouter:rpcrouter /app

USER rpcrouter
EXPOSE 8545
VOLUME /app/data

# 可选覆盖：RPCROUTER_CONFIG 指向挂载的配置文件；RPCROUTER_* 覆写关键项。
ENV RPCROUTER_CONFIG=/app/config.toml
ENV RPCROUTER_CHAINLIST_CACHE_PATH=/app/data/rpcs.json

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8545/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/rpcrouter"]
