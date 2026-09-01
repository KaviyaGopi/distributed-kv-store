FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o /out/kvnode ./cmd/kvnode
RUN CGO_ENABLED=0 go build -o /out/kv-events-consumer ./cmd/kv-events-consumer

FROM alpine:3.20

RUN apk add --no-cache ca-certificates
COPY --from=build /out/kvnode /usr/local/bin/kvnode
COPY --from=build /out/kv-events-consumer /usr/local/bin/kv-events-consumer

ENTRYPOINT ["/usr/local/bin/kvnode"]
