FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod files first
COPY go.mod go.sum ./

# Download dependencies (this layer will be cached)
RUN go mod download

# Copy source code
COPY pkg/ pkg/
COPY cmd/ cmd/

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o dbtune-agent ./cmd/agent.go

# Use a minimal image for the final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/dbtune-agent .

# Run the binary
ENTRYPOINT ["./dbtune-agent"]
