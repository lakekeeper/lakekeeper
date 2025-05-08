FROM rust:1.86-slim-bookworm AS chef

ARG NO_CHEF=false
ENV NO_CHEF=${NO_CHEF}

ENV NODE_VERSION=23.3.0
ENV NVM_DIR=/root/.nvm

# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN apt-get update -qq && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yqq cmake libclang1 clang llvm curl build-essential libpq-dev pkg-config libgssapi-krb5-2 libssl-dev make perl wget zip unzip --no-install-recommends && \
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && \
  . "$NVM_DIR/nvm.sh" && nvm install ${NODE_VERSION}  && \
  . "$NVM_DIR/nvm.sh" && nvm use v${NODE_VERSION}  && \
  . "$NVM_DIR/nvm.sh" && nvm alias default v${NODE_VERSION}  && \
  node -v && npm -v

ENV PATH="/root/.nvm/versions/node/v${NODE_VERSION}/bin/:${PATH}"

RUN $NO_CHEF || cargo install cargo-chef

WORKDIR /app

FROM chef AS planner
COPY . .
RUN ($NO_CHEF && touch recipe.json) || cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN $NO_CHEF || cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .

ENV SQLX_OFFLINE=true
RUN cargo build --release --all-features --bin iceberg-catalog

# DEPENDENCIES FOR KERBEROS / HDFS
FROM debian:bookworm-slim AS kerberos-deb-extractor
WORKDIR /tmp
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN echo "deb http://deb.debian.org/debian bookworm-backports main" >> /etc/apt/sources.list && \
  apt-get update && \
  apt-get download \
  libssl3 \
  ca-certificates \
  libgssapi-krb5-2 \
  libkrb5-3 \
  libk5crypto3 \
  libcom-err2 \
  libkrb5support0 \
  libkeyutils1 \
  && \
  mkdir -p /dpkg/var/lib/dpkg/status.d/ && \
  for deb in *.deb; do \
  package_name=$(dpkg-deb -I "${deb}" | awk '/^ Package: .*$/ {print $2}'); \
  echo "Processing: ${package_name}"; \
  dpkg --ctrl-tarfile "$deb" | tar -Oxf - ./control > "/dpkg/var/lib/dpkg/status.d/${package_name}"; \
  dpkg --extract "$deb" /dpkg || exit 10; \
  done
RUN find /dpkg/ -type d -empty -delete && \
  rm -r /dpkg/usr/share/doc/

# REMOVE UNUSED FILES FROM DISTROLLESS IMAGE
# (no rm in distroless)
FROM gcr.io/distroless/cc-debian12:nonroot as base
FROM debian:bookworm as cleaner
COPY --from=base / /clean
RUN rm -r \
  /clean/usr/lib/*-linux-gnu/libgomp*  \
  /clean/usr/lib/*-linux-gnu/libssl*  \
  /clean/usr/lib/*-linux-gnu/libstdc++* \
  /clean/usr/lib/*-linux-gnu/engines-3 \
  /clean/usr/lib/*-linux-gnu/ossl-modules \
  /clean/usr/lib/*-linux-gnu/libcrypto.so.3 \
  /clean/usr/lib/*-linux-gnu/gconv \
  /clean/var/lib/dpkg/status.d/libgomp1*  \
  /clean/var/lib/dpkg/status.d/libssl3*  \
  /clean/var/lib/dpkg/status.d/libstdc++6* \
  /clean/usr/share/doc/*

# BUILD THE FINAL IMAGE
FROM scratch
ARG EXPIRES=Never
LABEL maintainer="moderation@vakamo.com" quay.expires-after=${EXPIRES}
COPY --from=cleaner /clean /
COPY --from=kerberos-deb-extractor /dpkg /
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
