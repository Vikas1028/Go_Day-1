package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm"
)

const (
	transactionProducerType = "kafka-producer"
	spanWriteMessageType    = "WriteMessage"
)

func main() {
	// Elastic APM initialization
	apmServerURL := os.Getenv("ELASTIC_APM_SERVER_URL")
	if apmServerURL == "" {
		log.Fatalf("ELASTIC_APM_SERVER_URL environment variable not set")
	}
	fmt.Println(apmServerURL)
	tracer, err := apm.NewTracer("apm_Demo", "http://localhost:8200")
	if err != nil {
		log.Fatalf("Failed to create the APM tracer: %v", err)
	}
	defer tracer.Close()

	// Set up the Kafka writer
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"}, // Change to your Kafka broker address
		Topic:   "apm_kafka",                // Change to your Kafka topic
	})

	// Produce Kafka messages
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value%d", i)),
		}

		tx := apm.DefaultTracer.StartTransaction("KafkaProducerTransaction", transactionProducerType)

		span, _ := apm.StartSpan(ctx, "Produce Kafka Message", spanWriteMessageType)
		span.Context.SetLabel("kafka_topic", kafkaWriter.Topic)
		span.Context.SetLabel("kafka_key", string(message.Key))
		span.Context.SetLabel("kafka_value", string(message.Value))

		err := kafkaWriter.WriteMessages(ctx, message)
		if err != nil {
			apm.CaptureError(ctx, err).Send()
		}

		span.End()
		tx.End()

		fmt.Printf("Produced message %d\n", i)
	}

	// Clean up resources
	kafkaWriter.Close()
}
