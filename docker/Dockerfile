# Stage 1: Build Next.js static export
# Use host platform to avoid QEMU emulation — output is platform-independent static assets
FROM --platform=$BUILDPLATFORM node:20-alpine AS web-builder
WORKDIR /app/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# Stage 2: Build Go binary
FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache gcc musl-dev sqlite-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=1
RUN go build -o /devcloud ./cmd/devcloud
RUN go build -o /codegen ./cmd/codegen

# Stage 3: Runtime
FROM alpine:3.20
RUN apk add --no-cache sqlite-libs ca-certificates su-exec
RUN adduser -D -H -h /app appuser
WORKDIR /app
COPY --from=go-builder /devcloud /app/devcloud
COPY --from=go-builder /codegen /app/codegen
COPY --from=web-builder /app/web/out /app/web/out
COPY smithy-models/ /app/smithy-models/
COPY internal/codegen/templates/ /app/templates/
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chown -R appuser:appuser /app

EXPOSE 4747
VOLUME /app/data

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/devcloud"]
