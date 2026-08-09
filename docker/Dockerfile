# Stage 1: Build Go binary
FROM golang:1.26-alpine AS go-builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# No C toolchain, no SQLite headers: the driver is pure Go. That is the same
# property that lets the released binaries build without one.
ENV CGO_ENABLED=0
RUN go build -o /devcloud ./cmd/devcloud
RUN go build -o /codegen ./cmd/codegen

# Stage 2: Runtime
FROM alpine:3.20
RUN apk add --no-cache ca-certificates su-exec
RUN adduser -D -H -h /app appuser
WORKDIR /app
COPY --from=go-builder /devcloud /app/devcloud
COPY --from=go-builder /codegen /app/codegen
COPY smithy-models/ /app/smithy-models/
COPY internal/codegen/templates/ /app/templates/
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chown -R appuser:appuser /app

EXPOSE 4747
VOLUME /app/data

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/devcloud"]
