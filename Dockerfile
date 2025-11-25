# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies for CGO (required for SQLite)
RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build both applications with CGO enabled and FTS5 support
RUN CGO_ENABLED=1 go build --tags "fts5" -o /crawl cmd/crawl/*.go
RUN CGO_ENABLED=1 go build --tags "fts5" -o /sync cmd/sync/*.go

# Runtime stage
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache sqlite-libs ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /crawl /app/crawl
COPY --from=builder /sync /app/sync

# Create data directory for SQLite
RUN mkdir -p /data

# Default command runs the crawler
CMD ["/app/crawl"]
