FROM rust:alpine

RUN apk update && apk upgrade
RUN apk add --no-cache ca-certificates gcc llvm clang19 clang19-libclang build-base curl perl nodejs npm git bash cmake pkgconf python3 linux-headers zstd-dev

WORKDIR /build
ENV SQLX_OFFLINE=true
ENV RUST_BACKTRACE=full
