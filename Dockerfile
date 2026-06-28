FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /out/farias-proxy-rotator .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    addgroup -S app && \
    adduser -S app -G app

WORKDIR /app

COPY --from=builder /out/farias-proxy-rotator /usr/local/bin/farias-proxy-rotator

USER app

EXPOSE 3000

ENTRYPOINT ["farias-proxy-rotator"]
