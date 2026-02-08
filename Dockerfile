# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o breeze ./cmd/breeze/main.go

# Final stage
FROM alpine:latest

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/breeze .

# Create data directory
RUN mkdir -p /root/data

# Expose ports
EXPOSE 8080

# Command to run
ENTRYPOINT ["./breeze"]
CMD ["start", "--path", "./data", "--port", "8080"]
