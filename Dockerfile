FROM golang:1.23-alpine

WORKDIR /app
COPY . .

RUN go build -o dbtune-agent ./cmd/agent.go

ENTRYPOINT ["/app/dbtune-agent"]
