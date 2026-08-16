# syntax=docker/dockerfile:1.7

FROM rust:1.90-alpine AS builder

RUN apk add --no-cache musl-dev
WORKDIR /build/ThreadWeave

# Keep dependency compilation cached when only application sources change.
COPY ThreadWeave/Cargo.toml ThreadWeave/Cargo.lock ./
COPY ThreadWeave/src ./src

RUN --mount=type=cache,id=threadweave-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=threadweave-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=threadweave-target,target=/build/ThreadWeave/target \
    cargo build --locked --release --bin threadweave && \
    cp target/release/threadweave /threadweave && \
    strip /threadweave

FROM scratch AS runtime

COPY --from=builder /threadweave /threadweave
COPY ThreadWeave/docker/threadweave.yaml /etc/threadweave/threadweave.yaml

USER 65532:65532
EXPOSE 50051
ENTRYPOINT []
CMD ["/threadweave", "server", "--config", "/etc/threadweave/threadweave.yaml"]
