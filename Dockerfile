ARG GO_VERSION=1.26.2
ARG GO_IMAGE=golang:${GO_VERSION}-bookworm
ARG GOPROXY=https://goproxy.cn,direct

FROM ${GO_IMAGE} AS build

ARG GOPROXY
ENV GOPROXY=${GOPROXY}

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN cat > /tmp/healthcheck.go <<'EOF'
package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:8888/healthz")
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		os.Exit(1)
	}
}
EOF
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aieas-backend . && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aieas-healthcheck /tmp/healthcheck.go

FROM scratch

WORKDIR /app

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /out/aieas-backend /usr/local/bin/aieas-backend
COPY --from=build /out/aieas-healthcheck /usr/local/bin/aieas-healthcheck
COPY configs ./configs

ENV TZ=Asia/Shanghai

USER 65532:65532

EXPOSE 8888

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/aieas-healthcheck"]

ENTRYPOINT ["/usr/local/bin/aieas-backend"]
