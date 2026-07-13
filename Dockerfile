# syntax=docker/dockerfile:1.7

ARG S5CMD_VERSION=v2.3.0
FROM --platform=$TARGETPLATFORM peakcom/s5cmd:${S5CMD_VERSION} AS s5cmd

FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS go-deps
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download

FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
COPY --from=go-deps /go/pkg/mod /go/pkg/mod
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOPROXY=off CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -mod=readonly -o /out/partforge ./cmd/partforge

FROM ubuntu:24.04 AS clickhouse-runtime
ARG DEBIAN_FRONTEND=noninteractive
ARG CLICKHOUSE_VERSION=26.6.1.1193

RUN apt-get update \
    && apt-get install -y --no-install-recommends apt-transport-https ca-certificates curl gnupg tzdata \
    && curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && arch="$(dpkg --print-architecture)" \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${arch}] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        clickhouse-common-static=${CLICKHOUSE_VERSION} \
        clickhouse-server=${CLICKHOUSE_VERSION} \
        clickhouse-client=${CLICKHOUSE_VERSION} \
    && rm -rf /var/lib/apt/lists/*

FROM clickhouse-runtime AS clickhouse-util-udfs
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-yaml \
    && rm -rf /var/lib/apt/lists/*
COPY clickhouse-util-udfs.yml /etc/partforge/clickhouse-util-udfs.yml
COPY clickhouse/install_clickhouse_util_udfs.py /usr/local/bin/install-clickhouse-util-udfs
RUN chmod 0755 /usr/local/bin/install-clickhouse-util-udfs \
    && CLICKHOUSE_CONFIG_DIR=/out/etc/clickhouse-server CLICKHOUSE_DATA_PATH=/out/var/lib/clickhouse \
        install-clickhouse-util-udfs /etc/partforge/clickhouse-util-udfs.yml "$TARGETARCH"

FROM clickhouse-runtime AS worker
COPY --from=build /out/partforge /usr/local/bin/partforge
COPY --from=s5cmd /s5cmd /usr/local/bin/s5cmd
COPY --from=clickhouse-util-udfs --chown=clickhouse:clickhouse /out/etc/clickhouse-server/config.d/clickhouse-util-udfs.xml /etc/clickhouse-server/config.d/clickhouse-util-udfs.xml
COPY --from=clickhouse-util-udfs --chown=clickhouse:clickhouse /out/etc/clickhouse-server/user_defined/ /etc/clickhouse-server/user_defined/
COPY --from=clickhouse-util-udfs --chown=clickhouse:clickhouse /out/var/lib/clickhouse/user_scripts/ /var/lib/clickhouse/user_scripts/
RUN chmod 0755 /usr/local/bin/partforge /usr/local/bin/s5cmd
USER root
ENTRYPOINT ["/usr/local/bin/partforge"]
CMD ["worker"]
