FROM rust:1.85-slim-bookworm AS chef

ARG NO_CHEF=false
ENV NO_CHEF=${NO_CHEF}

ENV NODE_VERSION=23.3.0
ENV NVM_DIR=/root/.nvm

# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN apt-get update -qq && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yqq cmake curl build-essential libpq-dev pkg-config make perl wget zip unzip --no-install-recommends && \
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
LABEL maintainer="moderation@vakamo.com" quay.expires-after=${EXPIRES}

COPY --from=cleaner /clean /

# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
