FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" -o /chronicle ./cmd/chronicle

FROM gcr.io/distroless/static-debian12

COPY --from=builder /chronicle /chronicle

EXPOSE 8086

ENTRYPOINT ["/chronicle", "serve"]
