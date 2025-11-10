FROM golang:1.25-alpine AS builder

WORKDIR /app

# Build arguments for version information
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

# Copy go mod files first
COPY go.mod go.sum ./

# Download dependencies (this layer will be cached)
RUN go mod download

# Copy source code
COPY pkg/ pkg/
COPY cmd/ cmd/

# Build the application with version information
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "\
        -X github.com/dbtuneai/agent/pkg/version.Version=${VERSION} \
        -X github.com/dbtuneai/agent/pkg/version.Commit=${COMMIT} \
        -X github.com/dbtuneai/agent/pkg/version.Date=${DATE} \
    " \
    -o dbtune-agent ./cmd/agent.go

# Use a minimal image for the final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/dbtune-agent .

# Run the binary
ENTRYPOINT ["./dbtune-agent"]
