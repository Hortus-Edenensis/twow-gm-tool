FROM docker.io/library/rust:1.75-bookworm AS build
WORKDIR /work

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates mariadb-client \
  && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /usr/sbin/nologin appuser
WORKDIR /app

COPY --from=build /work/target/release/twow-gm-tool /usr/local/bin/twow-gm-tool

USER appuser
EXPOSE 8080

ENV GM_TOOL_BIND_ADDR=0.0.0.0:8080

ENTRYPOINT ["/usr/local/bin/twow-gm-tool"]
