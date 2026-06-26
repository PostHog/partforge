# syntax=docker/dockerfile:1.7

ARG S5CMD_VERSION=v2.3.0
FROM --platform=$TARGETPLATFORM peakcom/s5cmd:${S5CMD_VERSION} AS s5cmd

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS go-deps
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
COPY --from=go-deps /go/pkg/mod /go/pkg/mod
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOPROXY=off CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -mod=readonly -o /out/partforge ./cmd/partforge

FROM ubuntu:24.04 AS clickhouse-runtime
ARG DEBIAN_FRONTEND=noninteractive
ARG CLICKHOUSE_VERSION=26.3.10.60

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

FROM clickhouse-runtime AS worker
COPY --from=build /out/partforge /usr/local/bin/partforge
COPY --from=s5cmd /s5cmd /usr/local/bin/s5cmd
RUN chmod 0755 /usr/local/bin/partforge /usr/local/bin/s5cmd
USER root
ENTRYPOINT ["/usr/local/bin/partforge"]
CMD ["worker"]
