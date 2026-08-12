# syntax=docker/dockerfile:1.7

FROM rust:1.90-alpine AS builder

RUN apk add --no-cache musl-dev
WORKDIR /build/ThreadWeave

# Keep dependency compilation cached when only application sources change.
COPY ThreadWeave/Cargo.toml ThreadWeave/Cargo.lock ./
COPY ThreadWeave/.cargo ./.cargo
COPY ThreadWeave/src ./src

RUN --mount=type=secret,id=buf_token,env=BUF_TOKEN \
    --mount=type=cache,id=threadweave-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=threadweave-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=threadweave-target,target=/build/ThreadWeave/target \
    CARGO_REGISTRIES_BUF_TOKEN="Bearer ${BUF_TOKEN}" cargo build --locked --release --bins && \
    cp target/release/threadweave-api /threadweave-api && \
    cp target/release/threadweave-scheduler /threadweave-scheduler && \
    cp target/release/threadweave-worker /threadweave-worker && \
    strip /threadweave-api /threadweave-scheduler /threadweave-worker

FROM scratch AS runtime

COPY --from=builder /threadweave-api /threadweave-api
COPY --from=builder /threadweave-scheduler /threadweave-scheduler
COPY --from=builder /threadweave-worker /threadweave-worker
COPY ThreadWeave/docker/threadweave.yaml /etc/threadweave/threadweave.yaml

USER 65532:65532
EXPOSE 50051
ENTRYPOINT []
CMD ["/threadweave-api", "--config", "/etc/threadweave/threadweave.yaml"]
