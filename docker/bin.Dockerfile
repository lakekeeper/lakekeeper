ARG ARCH

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

FROM gcr.io/distroless/cc-debian12:nonroot-${ARCH} as base

FROM busybox:1.37.0 as cleaner
# small diversion through busybox to remove some files

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

FROM scratch as lakekeeper
ARG BIN
ARG EXPIRES=Never
LABEL maintainer="moderation@vakamo.com" quay.expires-after=${EXPIRES}
COPY --from=cleaner /clean /
# copy the build artifact from the build stage
COPY ${BIN} /home/nonroot/iceberg-catalog
# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]

FROM lakekeeper as lakekeeper-hdfs
COPY --from=kerberos-deb-extractor /dpkg /
