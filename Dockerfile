# Stage 1: Build
FROM golang:1.24-alpine AS build

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /bin/server ./cmd/server/main.go

# Stage 2: Runtime image (<20MB)
FROM alpine:3.20

WORKDIR /app

RUN addgroup -S erp && adduser -S erp -G erp
USER erp:erp

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /bin/server /app/server

EXPOSE 8087

ENTRYPOINT ["/app/server"]
