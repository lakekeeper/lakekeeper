# FROM alpine:3.21
FROM rust:alpine

RUN apk update && apk upgrade
RUN apk add --no-cache ca-certificates gcc build-base curl perl nodejs npm git bash cmake pkgconf python3 linux-headers

# ENV RUSTUP_HOME=/usr/local/rustup \
#   CARGO_HOME=/usr/local/cargo \
#   PATH=/usr/local/cargo/bin:$PATH \
#   RUST_VERSION=1.85.0

# RUN set -eux; \
#   apkArch="$(apk --print-arch)"; \
#   case "$apkArch" in \
#   x86_64) rustArch='x86_64-unknown-linux-musl'; rustupSha256='1455d1df3825c5f24ba06d9dd1c7052908272a2cae9aa749ea49d67acbe22b47' ;; \
#   aarch64) rustArch='aarch64-unknown-linux-musl'; rustupSha256='7087ada906cd27a00c8e0323401a46804a03a742bd07811da6dead016617cc64' ;; \
#   *) echo >&2 "unsupported architecture: $apkArch"; exit 1 ;; \
#   esac; \
#   url="https://static.rust-lang.org/rustup/archive/1.27.1/${rustArch}/rustup-init"; \
#   wget "$url"; \
#   echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
#   chmod +x rustup-init; \
#   ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
#   rm rustup-init; \
#   chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
#   rustup --version; \
#   cargo --version; \
#   rustc --version;


WORKDIR /build
ENV SQLX_OFFLINE=true
ENV RUST_BACKTRACE=full
