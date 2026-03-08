FROM golang:1.25.5-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG REVISION=unknown
ARG BRANCH=unknown
ARG BUILD_DATE
RUN CGO_ENABLED=0 go build \
    -ldflags "-X 'github.com/hacktohell/opex/internal/api.Version=${VERSION}' \
              -X 'github.com/hacktohell/opex/internal/api.Revision=${REVISION}' \
              -X 'github.com/hacktohell/opex/internal/api.Branch=${BRANCH}' \
              -X 'github.com/hacktohell/opex/internal/api.BuildDate=${BUILD_DATE}'" \
    -o /opex ./cmd/opex

FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY --from=builder /opex /usr/local/bin/opex
EXPOSE 8080
ENTRYPOINT ["opex"]
CMD ["--config", "/etc/opex/config.yaml"]
