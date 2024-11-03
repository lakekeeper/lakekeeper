FROM rust:1.82-slim-bookworm AS chef
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq curl build-essential libpq-dev pkg-config libssl-dev make perl wget zip unzip --no-install-recommends && \
    cargo install -q --version=0.8.2 sqlx-cli --no-default-features --features postgres

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v28.2/protoc-28.2-linux-x86_64.zip && \
    unzip protoc-28.2-linux-x86_64.zip -d /usr/local/ && \
    rm protoc-28.2-linux-x86_64.zip
RUN cargo install cargo-chef 

WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .

ENV SQLX_OFFLINE=true
RUN cargo build --release --bin iceberg-catalog

# our final base
FROM gcr.io/distroless/cc-debian12:nonroot as base


FROM busybox:1.37.0 as cleaner
# small diversion through busybox to remove some files
# (no rm in distroless)

COPY --from=base / /clean

RUN rm -r /clean/usr/lib/*-linux-gnu/libgomp*  \
         /clean/usr/lib/*-linux-gnu/libssl*  \
         /clean/usr/lib/*-linux-gnu/libstdc++* \
         /clean/usr/lib/*-linux-gnu/engines-3 \
         /clean/usr/lib/*-linux-gnu/ossl-modules \
         /clean/usr/lib/*-linux-gnu/libcrypto.so.3 \
        /clean/usr/lib/*-linux-gnu/gconv \
       /clean/var/lib/dpkg/status.d/libgomp1*  \
       /clean/var/lib/dpkg/status.d/libssl3*  \
       /clean/var/lib/dpkg/status.d/libstdc++6* \
       /clean/usr/share/doc/libssl3 \
       /clean/usr/share/doc/libstdc++6 \
       /clean/usr/share/doc/libgomp1


FROM scratch

ARG EXPIRES=Never
LABEL maintainer="moderation@hansetag.com" quay.expires-after=${EXPIRES}

COPY --from=cleaner /clean /

# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
