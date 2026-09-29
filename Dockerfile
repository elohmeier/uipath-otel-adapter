FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/adapter ./cmd/uipath-otel-adapter && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/mock ./cmd/mock-orchestrator

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates && addgroup -g 10001 adapter && adduser -D -u 10001 -G adapter adapter && \
    mkdir /data && chown adapter:adapter /data
USER 10001:10001
WORKDIR /app
COPY --from=build /out/adapter /usr/local/bin/uipath-otel-adapter
ENV STATE_PATH=/data/state.db LISTEN_ADDRESS=0.0.0.0:8088
EXPOSE 8088
ENTRYPOINT ["uipath-otel-adapter"]

FROM runtime AS mock
COPY --from=build /out/mock /usr/local/bin/mock-orchestrator
ENTRYPOINT ["mock-orchestrator"]
