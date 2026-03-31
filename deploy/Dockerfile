FROM golang:1.25.5-bookworm AS builder

WORKDIR /src

# Cache dependency downloads across builds
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Cache the Go build cache across builds
COPY . .

ARG VERSION=dev
ARG REVISION=unknown
ARG BRANCH=unknown
ARG BUILD_DATE
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "\
              -X 'github.com/hacktohell/opex/internal/api.Version=${VERSION}' \
              -X 'github.com/hacktohell/opex/internal/api.Revision=${REVISION}' \
              -X 'github.com/hacktohell/opex/internal/api.Branch=${BRANCH}' \
              -X 'github.com/hacktohell/opex/internal/api.BuildDate=${BUILD_DATE}'" \
    -o /opex ./cmd/opex

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /opex /usr/local/bin/opex
EXPOSE 8080
ENTRYPOINT ["opex"]
CMD ["--config", "/etc/opex/config.yaml"]
