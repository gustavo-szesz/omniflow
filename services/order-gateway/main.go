package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Item struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	PriceCents int    `json:"price_cents"`
}

type OrderRequest struct {
	CustomerID    string `json:"customer_id"`
	CustomerEmail string `json:"customer_email"`
	AmountCents   int    `json:"amount_cents"`
	Currency      string `json:"currency"`
	Items         []Item `json:"items"`
}

type OrderData struct {
	OrderID       string `json:"order_id"`
	CustomerID    string `json:"customer_id"`
	CustomerEmail string `json:"customer_email"`
	AmountCents   int    `json:"amount_cents"`
	Currency      string `json:"currency"`
}
type OrderEvent struct {
	EventID       string    `json:"event_id"`
	CorrelationID string    `json:"correlation_id"`
	Timestamp     string    `json:"timestamp"`
	Order         OrderData `json:"order"`
}

type App struct {
	rabbitChannel *amqp.Channel
}

func (app *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok", "service":"order-gateway"}`))
}

func (app *App) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	//decode json
	var req OrderRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, `{"error":"JSON invalid"}`, http.StatusBadRequest)
		return
	}

	if req.AmountCents <= 0 || req.CustomerEmail == "" || req.CustomerID == "" {
		http.Error(w, `{"error":"Required fields invalid (amount_cents, 
						customer_email, customer_id)"}`, http.StatusBadRequest)
		return
	}

	orderID := fmt.Sprintf("ord_%s", uuid.New().String())
	correlationID := fmt.Sprintf("corr_%s", uuid.New().String())

	event := OrderEvent{
		EventID:       fmt.Sprintf("evt_%s", uuid.New().String()),
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Order: OrderData{
			OrderID:       orderID,
			CustomerID:    req.CustomerID,
			CustomerEmail: req.CustomerEmail,
			AmountCents:   req.AmountCents,
			Currency:      req.Currency,
		},
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("Erro ao serializar evento: %v", err)
		http.Error(w, `{"error":"Erro interno ao processar evento"}`, http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = app.rabbitChannel.PublishWithContext(
		ctx,
		"",               // Exchange padrão (direct)
		"orders.created", // Routing key / Nome da fila
		false,            // Mandatory
		false,            // Immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Mensagem sobrevive a reinício do broker
			Body:         eventBytes,
		},
	)

	if err != nil {
		log.Printf("Erro ao publicar no RabbitMQ: %v", err)
		http.Error(w, `{"error":"Falha ao enviar mensagem para a fila"}`, http.StatusInternalServerError)
		return
	}

	log.Printf("[SUCESSO] Pedido %s publicado na fila orders.created (Correlation: %s)", orderID, correlationID)

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id":       orderID,
		"correlation_id": correlationID,
		"status":         "QUEUED",
	})

}

func main() {
	// 1. Obter URL do RabbitMQ do ambiente
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@192.168.1.9:5672/"
	}

	log.Printf("Conectando ao RabbitMQ em: %s ...", rabbitURL)
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Falha critica ao conectar no RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
	}
	defer ch.Close()

	// 2. Garantir que a fila 'orders.created' exista
	_, err = ch.QueueDeclare(
		"orders.created", // Nome da fila
		true,             // Durable (persiste se o RabbitMQ reiniciar)
		false,            // Auto-delete
		false,            // Exclusive
		false,            // No-wait
		nil,              // Arguments
	)
	if err != nil {
		log.Fatalf("Falha ao declarar fila no RabbitMQ: %v", err)
	}

	app := &App{rabbitChannel: ch}

	// 3. Configurar rotas usando ServeMux da biblioteca padrão
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.handleHealth)
	mux.HandleFunc("POST /orders", app.handleCreateOrder)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Order Gateway escutando na porta %s...", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Erro no servidor HTTP: %v", err)
	}
}
