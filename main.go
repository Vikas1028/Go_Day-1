package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm"
	"go.elastic.co/apm/module/apmhttp"
	//"go.elastic.co/apm/stacktrace"
)

const (
	transactionProducerType = "kafka-producer"
	spanWriteMessageType    = "WriteMessage"
)

func main() {
	// Elastic APM initialization
	tracer, err := apm.NewTracer("", "")
	if err != nil {
		log.Fatalf("Failed to create the APM tracer: %v", err)
	}
	defer tracer.Close()

	// Set up the Kafka writer
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"}, // Change to your Kafka broker address
		Topic:   "test-topic",               // Change to your Kafka topic
	})

	// Wrap the Kafka writer with APM tracing
	apmKafkaWriter := WrapWriter(kafkaWriter)

	// Produce Kafka messages
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value%d", i)),
		}

		err := apmKafkaWriter.WriteMessages(ctx, message)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
		}

		fmt.Printf("Produced message %d\n", i)
	}

	// Clean up resources
	apmKafkaWriter.W.Close()
}

// Writer is a wrapper around kafka.Writer
type Writer struct {
	W *kafka.Writer
}

// WrapWriter returns a new Writer wrapper around kafka.Writer
func WrapWriter(w *kafka.Writer) *Writer {
	return &Writer{
		W: w,
	}
}

// WriteMessages writes a message to Kafka
func (w *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		tx = apm.DefaultTracer.StartTransaction("Produce "+w.W.Topic, transactionProducerType)
		defer tx.End()
	}
	ctx = apm.ContextWithTransaction(ctx, tx)
	for i := range msgs {
		span, _ := apm.StartSpan(ctx, "Produce "+string(msgs[i].Key), spanWriteMessageType)
		span.Context.SetLabel("topic", w.W.Topic)
		span.Context.SetLabel("key", string(msgs[i].Key))
		//span.Context.SetStacktrace(stacktrace.Callers(0))
		traceParent := apmhttp.FormatTraceparentHeader(span.TraceContext())
		fmt.Println("traceparent", traceParent)
		w.addTraceparentHeader(&msgs[i], traceParent)
		span.End()
	}
	err := w.W.WriteMessages(ctx, msgs...)
	if err != nil {
		apm.CaptureError(ctx, err).Send()
	}
	return err
}

func (w *Writer) addTraceparentHeader(msg *kafka.Message, traceParent string) {
	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   "traceparent",
		Value: []byte(traceParent),
	})
}
