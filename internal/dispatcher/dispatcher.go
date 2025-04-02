package dispatcher

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/johandrevandeventer/kafkaclient/payload"
	"github.com/johandrevandeventer/mqtt-to-kafka/internal/flags"
	coreutils "github.com/johandrevandeventer/mqtt-to-kafka/utils"
	"go.uber.org/zap"
)

type Producer interface {
	SendMessage(ctx context.Context, topic string, payload []byte) error
}

type Dispatcher struct {
	producer Producer
	topics   map[string]string
	logger   *zap.Logger
}

func NewDispatcher(producer Producer, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		producer: producer,
		topics:   make(map[string]string),
		logger:   logger,
	}
}

func (d *Dispatcher) RegisterKafkaTopic(topicPrefix, kafkaTopic string) {
	if flags.FlagEnvironment == "development" {
		kafkaTopic = fmt.Sprintf("%s_%s", kafkaTopic, flags.FlagEnvironment)
	}

	d.topics[topicPrefix] = kafkaTopic
	d.logger.Debug("Registered Kafka topic", zap.String("topicPrefix", topicPrefix), zap.String("kafkaTopic", kafkaTopic))
}

// Start starts the dispatcher, reading from the supplied MQTT message channel.
func (d *Dispatcher) Start(messageChannel chan mqtt.Message) {
	go func() {
		for msg := range messageChannel {
			d.dispatch(msg)
		}
	}()
}

// dispatch sends the message to the correct worker based on the topic.
func (d *Dispatcher) dispatch(msg mqtt.Message) {
	for topicPrefix, kafkaTopic := range d.topics {
		if len(msg.Topic()) >= len(topicPrefix) && msg.Topic()[:len(topicPrefix)] == topicPrefix {
			message := payload.Payload{
				ID:               coreutils.GenerateUUID(),
				MqttTopic:        msg.Topic(),
				Message:          msg.Payload(),
				MessageTimestamp: time.Now(),
			}
			serializedMessage, err := message.Serialize()
			if err != nil {
				d.logger.Error(fmt.Sprintf("Failed to serialize message: %v", err))
				return
			}

			// Check if serialized message is empty
			if len(serializedMessage) == 0 {
				d.logger.Error("Serialized message is empty")
				return
			}

			d.producer.SendMessage(context.Background(), kafkaTopic, serializedMessage)
			return
		}
	}
	d.logger.Error(fmt.Sprintf("No Kafka topic found for topic: %s", msg.Topic()))
}
