FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY . .

RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    echo "Configuring git to use token: $GITHUB_TOKEN" && \
    git config --global url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/" && \
    go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o gisit-realtime-backend cmd/api/main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/gisit-realtime-backend .

CMD ["./gisit-realtime-backend"]