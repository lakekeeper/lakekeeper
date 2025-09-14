FROM confluentinc/cp-kafka-connect:8.0.0 AS downloader

ARG CONFLUENT_HUB_ICEBERG_SINK_VERSION=1.9.2
ARG CONFLUENT_HUB_AVRO_VERSION=8.0.0

RUN confluent-hub install --no-prompt iceberg/iceberg-kafka-connect:${CONFLUENT_HUB_ICEBERG_SINK_VERSION} && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:${CONFLUENT_HUB_AVRO_VERSION}

FROM quay.io/strimzi/kafka:0.47.0-kafka-4.0.0 AS connector
USER root:root
COPY --from=downloader /usr/share/confluent-hub-components /opt/kafka/plugins/
USER 1001
