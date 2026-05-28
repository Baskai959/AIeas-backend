# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.2

FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aieas-backend .

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl tzdata && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r aieas && \
    useradd -r -g aieas -d /app -s /usr/sbin/nologin aieas

WORKDIR /app

COPY --from=build /out/aieas-backend /usr/local/bin/aieas-backend
COPY configs ./configs

ENV TZ=Asia/Shanghai

USER aieas

EXPOSE 8888

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8888/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/aieas-backend"]
