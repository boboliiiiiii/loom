# Multi-stage build for the loom-sidecar FUSE binary.

FROM rust:1.93-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
        pkg-config \
        libfuse3-dev \
        && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release --locked || cargo build --release
RUN strip target/release/loom-sidecar

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
        fuse3 \
        libfuse3-3 \
        && rm -rf /var/lib/apt/lists/* \
    && useradd -u 1000 -m loom \
    # /etc/mtab: fusermount3 wants to update it; symlink to /proc keeps it readable
    && ln -sf /proc/self/mounts /etc/mtab \
    # allow non-root users to use the `allow_other` mount option
    && echo "user_allow_other" >> /etc/fuse.conf

COPY --from=builder /app/target/release/loom-sidecar /usr/local/bin/loom-sidecar

# Runtime requirements (set by the k8s pod spec, not in the image):
#   securityContext.runAsUser:  1000   (matches USER below)
#   securityContext.runAsGroup: 1000
#   securityContext.fsGroup:    1000   (so kubelet-managed emptyDirs are accessible)
#   securityContext.capabilities.add: ["SYS_ADMIN"]   (required to mount FUSE)
#   /dev/fuse exposed as a volumeDevice or hostPath CharDevice
#
# `privileged: true` works as well but is unnecessarily broad — prefer the
# narrow cap + device + matching uid/gid setup above.
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/loom-sidecar"]
