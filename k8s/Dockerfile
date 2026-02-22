# Multi-architecture build
# Build: docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 -t chronicle .
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /chronicle ./cmd/chronicle

FROM gcr.io/distroless/static-debian12

LABEL org.opencontainers.image.source="https://github.com/chronicle-db/chronicle"
LABEL org.opencontainers.image.description="Chronicle embedded time-series database"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /chronicle /chronicle

EXPOSE 8086

ENTRYPOINT ["/chronicle", "serve"]
