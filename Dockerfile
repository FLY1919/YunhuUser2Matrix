# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/yhmatrix ./cmd/yhmatrix \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/yhprobe ./cmd/yhprobe

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 yhmatrix
WORKDIR /app
COPY --from=build /out/yhmatrix /out/yhprobe /usr/local/bin/
COPY config.example.yaml /app/config.example.yaml
USER yhmatrix
VOLUME ["/app/data"]
EXPOSE 8008
ENTRYPOINT ["yhmatrix"]
CMD ["-config", "/app/config.yaml"]
