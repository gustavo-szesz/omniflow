# Etapa 1: Compilação
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Baixar dependências primeiro (aproveita cache do Docker)
COPY go.mod go.sum ./
RUN go mod download

# Copiar código-fonte e compilar binário otimizado
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server main.go

# Etapa 2: Imagem final de produção
FROM alpine:3.20

WORKDIR /app
RUN apk --no-cache add ca-certificates

# Copiar apenas o binário compilado
COPY --from=builder /app/server /app/server

EXPOSE 8080

CMD ["/app/server"]