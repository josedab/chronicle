FROM golang:1.22-alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /chronicle
COPY go.mod go.sum ./
RUN go mod download
COPY . .
WORKDIR /chronicle/examples/edge-sync
RUN go build -o /edge-sync .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /edge-sync /usr/local/bin/edge-sync
ENTRYPOINT ["edge-sync"]
