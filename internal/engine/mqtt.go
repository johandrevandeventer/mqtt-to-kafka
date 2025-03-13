package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/johandrevandeventer/logging"
	"github.com/johandrevandeventer/mqtt-to-kafka/internal/flags"
	coreutils "github.com/johandrevandeventer/mqtt-to-kafka/utils"
	"github.com/johandrevandeventer/mqttclient"
	"go.uber.org/zap"
)

var (
	mqttStartTime time.Time
	mqttEndTime   time.Time
)

func (e *Engine) initMQTTClient() {
	e.logger.Info("Initializing MQTT client")
}

func (e *Engine) connectMQTTClient() error {
	config := mqttclient.MQTTConfig{
		Broker:                e.cfg.App.Mqtt.Broker,
		Port:                  e.cfg.App.Mqtt.Port,
		ClientID:              e.cfg.App.Mqtt.ClientId,
		Topic:                 e.cfg.App.Mqtt.Topic,
		Qos:                   e.cfg.App.Mqtt.Qos,
		CleanSession:          e.cfg.App.Mqtt.CleanSession,
		KeepAlive:             e.cfg.App.Mqtt.KeepAlive,
		ReconnectOnDisconnect: e.cfg.App.Mqtt.ReconnectOnFailure,
		Username:              e.cfg.App.Mqtt.Username,
		Password:              e.cfg.App.Mqtt.Password,
	}

	var mqttLogger *zap.Logger // Define mqttLogger outside if-else

	if flags.FlagMQTTLogging {
		mqttLogger = logging.GetLogger("mqtt")
	} else {
		mqttLogger = zap.NewNop() // Use zap.NewNop() to disable logging
	}

	e.mqttClient = mqttclient.NewMQTTClient(config, mqttLogger)
	if err := e.mqttClient.Connect(e.ctx); err != nil {
		return e.handleMqttConnectionError(err, config.Username, config.Password)
	}

	return nil
}

func (e *Engine) tryMQTTConnection(retryInterval int) {
	e.logger.Info("Attempting to connect to MQTT broker")

	if retryInterval > 60 {
		e.logger.Warn("Exceeded maximum retry interval of 60 seconds. Resetting to 60 seconds")
		retryInterval = 60
	}

	select {
	case <-e.ctx.Done():
		return
	case <-e.stopFileChan:
		return
	default:
		for {
			if e.mqttClient != nil && e.mqttClient.Client.IsConnected() {
				e.mqttClient.Disconnect()
			}

			if e.statePersister.Get("mqtt.status") == "connected" {
				e.mqttStatePersistStop()
			}

			if err := e.connectMQTTClient(); err != nil {
				e.logger.Error("Error connecting to MQTT broker", zap.Error(err))
				e.logger.Info(fmt.Sprintf("Retrying in %d seconds", retryInterval))

				// Retry logic with cancellation support
				for range rangeInt(retryInterval * 1000) {
					select {
					case <-e.ctx.Done():
						e.logger.Info("Context canceled, stopping MQTT connection attempts")
						return
					case <-e.stopFileChan:
						e.logger.Info("Received stop signal, stopping MQTT connection attempts")
						return
					default:
						time.Sleep(time.Millisecond)
					}
				}

				// Continue to the next iteration of the outer loop
				continue
			}

			// Subscription and persistence
			e.logger.Info("Connected to MQTT broker")
			e.mqttClient.Subscribe() // Add this line to subscribe to MQTT topics
			e.mqttStatePersistStart()
			break
		}
	}
}

// Handle MQTT connection error
func (e *Engine) handleMqttConnectionError(err error, username, password string) error {
	if strings.Contains(err.Error(), "bad user name or password") {
		if username == "" {
			return fmt.Errorf("bad MQTT credentials detected: No username provided")
		}
		if password == "" {
			return fmt.Errorf("bad MQTT credentials detected: No password provided")
		}
		return fmt.Errorf("bad MQTT credentials detected: %w", err)
	}
	return err
}

// mqttStatePersistStart persists the state of the MQTT connection
func (e *Engine) mqttStatePersistStart() {
	mqttStartTime = time.Now()

	coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: MQTT connection started\n", mqttStartTime.Format(time.RFC3339)))

	e.statePersister.Set("mqtt", map[string]interface{}{})
	e.statePersister.Set("mqtt.status", "connected")
	e.statePersister.Set("mqtt.start_time", mqttStartTime.Format(time.RFC3339))
	e.statePersister.Set("mqtt.topic", e.cfg.App.Mqtt.Topic)
	e.statePersister.Set("mqtt.client_id", e.mqttClient.Config.ClientID)
}

// mqttStatePersistStop persists the state of the MQTT connection
func (e *Engine) mqttStatePersistStop() {
	e.statePersister.Set("mqtt.status", "disconnected")

	if !mqttStartTime.IsZero() {
		mqttEndTime = time.Now()

		duration := mqttEndTime.Sub(mqttStartTime)

		coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: MQTT connection stopped\n", mqttEndTime.Format(time.RFC3339)))

		e.statePersister.Set("mqtt.end_time", mqttEndTime.Format(time.RFC3339))
		e.statePersister.Set("mqtt.duration", duration.String())
	}
}

func rangeInt(n int) []struct{} {
	return make([]struct{}, n)
}
