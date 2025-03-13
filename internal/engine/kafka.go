package engine

import (
	"log"

	"github.com/johandrevandeventer/kafkaclient/config"
	"github.com/johandrevandeventer/kafkaclient/producer"
	"github.com/johandrevandeventer/kafkaclient/prometheusserver"
	"github.com/johandrevandeventer/logging"
	"github.com/johandrevandeventer/mqtt-to-kafka/internal/dispatcher"
	"github.com/johandrevandeventer/mqtt-to-kafka/internal/flags"
	"github.com/johandrevandeventer/mqttclient"
	"go.uber.org/zap"
)

func (e *Engine) startKafkaProducer() {
	<-e.mqttConnectedCh

	e.logger.Info("Starting Kafka")

	var kafkaProducerLogger *zap.Logger
	if flags.FlagKafkaLogging {
		kafkaProducerLogger = logging.GetLogger("kafka.producer")
	} else {
		kafkaProducerLogger = zap.NewNop()
	}

	// Define Kafka producer config
	producerConfig := config.NewKafkaProducerConfig("localhost:9092", 5, 5)

	// Initialize Kafka Producer Pool
	kafkaProducerPool, err := producer.NewKafkaProducerPool(e.ctx, producerConfig, kafkaProducerLogger)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer pool: %v", err)
	}

	e.kafkaProducerPool = kafkaProducerPool
	// defer producerPool.Close()

	// Start Prometheus Metrics Server
	go prometheusserver.StartPrometheusServer(":2112", e.ctx)

	dispatcherInstance := dispatcher.NewDispatcher(e.kafkaProducerPool, e.logger)

	// Register Kafka topics
	dispatcherInstance.RegisterKafkaTopic("Rubicon/DSE", "rubicon_kafka_dse")
	dispatcherInstance.RegisterKafkaTopic("Rubicon/Lora", "rubicon_kafka_lora")

	// Start dispatcher
	dispatcherInstance.Start(mqttclient.GetMessageChannel())
}
