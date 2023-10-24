package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
	"github.com/sohaibomr/distributed-tracing/logger"
	"go.elastic.co/apm/module/apmgorilla/v2"
	"go.elastic.co/apm/module/apmhttp/v2"
	"go.elastic.co/apm/module/apmzap/v2"
	"go.elastic.co/apm/v2"
	"go.uber.org/zap"
)

func main() {
	fmt.Println("Starting server...")
	apmUrl := os.Getenv("ELASTIC_APM_SERVER_URL")
	apmUrl = "http://localhost:8200"
	fmt.Println(apmUrl)
	r := mux.NewRouter()
	apmgorilla.Instrument(r)
	r.HandleFunc("/payment", payment).Methods("POST")
	log.Fatal(http.ListenAndServe("localhost:8089", r))

}

type Order struct {
	ID       string `json:"order_id,omitempty"`
	Quantity int    `json:"quantity,omitempty"`
	Name     string `json:"name,omitempty"`
}

func payment(w http.ResponseWriter, r *http.Request) {
	traceContextFields := apmzap.TraceContext(r.Context())
	logger.Log.With(traceContextFields...).Info("Processing payment...")
	item := Order{}
	err := json.NewDecoder(r.Body).Decode(&item)
	if err != nil {
		logger.Log.With(traceContextFields...).Error(fmt.Sprintf("Error decoding request body: %s", err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	trans := newTransaction(item.ID)
	err = trans.processTransaction(r.Context())
	if err != nil {
		logger.Log.With(traceContextFields...).Error(fmt.Sprintf("Error processing transaction: %s", err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)

}

var Log = newLogger()

func newLogger() *zap.Logger {
	// zap logger config
	// config := zap.NewProductionConfig()
	// config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	// config.EncoderConfig.TimeKey = "timestamp"
	// config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	// config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	// logger, err := config.Build()
	// if err != nil {
	// 	panic(err)
	// }
	logger := zap.NewExample(zap.WrapCore((&apmzap.Core{}).WrapCore))
	return logger
}

type transaction struct {
	OrderID       string `json:"order_id"`
	TransactionId int    `json:"transaction_id"`
}

func newTransaction(orderId string) transaction {

	return transaction{
		OrderID:       orderId,
		TransactionId: rand.Intn(1000),
	}
}

func (t transaction) processTransaction(ctx context.Context) error {
	span, sCtx := apm.StartSpan(ctx, "processTransaction", "custom")
	span.Context.SetLabel("order_id", t.OrderID)
	span.Context.SetLabel("transaction_id", t.TransactionId)
	defer span.End()
	fmt.Printf("Processing transaction for order %s \n", t.OrderID)
	if t.OrderID == "" {
		err := fmt.Errorf("error processing transaction. Order ID is empty")
		apm.CaptureError(sCtx, err).Send()
		return err
	}
	return nil
}

const (
	transactionProducerType = "kafka-producer"
	spanWriteMessageType    = "WriteMessage"
)

// Writer is a wrapper around kafka.Writer
type Writer struct {
	W *kafka.Writer
}

// WrapWriter returns a new Writer wraper around kafka.Writer
func WrapWriter(w *kafka.Writer) *Writer {
	return &Writer{
		W: w,
	}
}

// WriteMessage writes a message to kafka
func (w *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		tx = apm.DefaultTracer().StartTransaction("Produce "+w.W.Topic, transactionProducerType)
		defer tx.End()
	}
	ctx = apm.ContextWithTransaction(ctx, tx)
	for i := range msgs {
		span, _ := apm.StartSpan(ctx, "Produce "+string(msgs[i].Key), spanWriteMessageType)
		span.Context.SetLabel("topic", w.W.Topic)
		span.Context.SetLabel("key", string(msgs[i].Key))
		traceParent := apmhttp.FormatTraceparentHeader(span.TraceContext())
		fmt.Println("traceparent", traceParent)
		w.addTraceparentHeader(&msgs[i], traceParent)
		span.End()
	}
	err := w.W.WriteMessages(ctx, msgs...)
	if err != nil {
		apm.CaptureError(ctx, err).Send()
	}
	// TODO: add panic recovery
	return err
}

func (w *Writer) addTraceparentHeader(msg *kafka.Message, traceParent string) {
	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   apmhttp.W3CTraceparentHeader,
		Value: []byte(traceParent),
	})
}
