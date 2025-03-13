package app

import (
	"os"
	"path/filepath"

	coreutils "github.com/johandrevandeventer/mqtt-to-kafka/utils"
)

var (
	appConfig *AppConfig

	// Default configurations
	defaultAppConfig     *AppConfig
	defaultRuntimeConfig *RuntimeConfig
	defaultLoggingConfig *LoggingConfig
	defaultMQTTConfig    *MqttConfig

	// File paths
	persistFilePath        = filepath.Join(coreutils.GetPersistDir(), "persist.json")
	loggingFilePath        = filepath.Join(coreutils.GetLoggingDir(), "app.jsonl")
	stopFileFilePath       = filepath.Join(coreutils.GetTmpDir(), "stop_signal")
	connectionsLogFilePath = filepath.Join(coreutils.GetConnectionsDir(), "connections.log")
)

func init() {
	// persistFilePath = filepath.Join(coreutils.GetPersistDir(), "persist.json")
	// loggingFilePath = filepath.Join(coreutils.GetLoggingDir(), "app.jsonl")
	// stopFileFilePath = filepath.Join(coreutils.GetTmpDir(), "stop_signal")
	// connectionsLogFilePath = filepath.Join(coreutils.GetConnectionsDir(), "connections.log")

	defaultRuntimeConfig = &RuntimeConfig{
		RootDir:                coreutils.GetRootDir(),
		TmpDir:                 coreutils.GetTmpDir(),
		PersistFilePath:        persistFilePath,
		StopFileFilepath:       stopFileFilePath,
		ConnectionsLogFilePath: connectionsLogFilePath,
	}

	defaultLoggingConfig = &LoggingConfig{
		Level:      "info",
		FilePath:   loggingFilePath,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
		AddTime:    true,
	}

	defaultMQTTConfig = &MqttConfig{
		Broker:             "broker.emqx.io",
		ClientId:           "bms-mqtt-worker-cli",
		Port:               1883,
		Topic:              "bms",
		Qos:                0,
		CleanSession:       true,
		KeepAlive:          60,
		ReconnectOnFailure: true,
		Username:           "",
		Password:           "",
	}

	defaultAppConfig = &AppConfig{
		Runtime: *defaultRuntimeConfig,
		Logging: *defaultLoggingConfig,
		Mqtt:    *defaultMQTTConfig,
	}

	appConfig = defaultAppConfig
}

// InitAppConfig initializes the app configuration
func InitAppConfig(filePath string) (fileExists bool, err error) {
	// Check if the config directory exists, if not create it
	if coreutils.FileExists(filePath) {
		return true, nil
	}

	// Create the configuration directory
	dir := filepath.Dir(filePath)
	os.Mkdir(dir, 0o770)

	// Save the app configuration
	err = SaveAppConfig(filePath, true)
	if err != nil {
		return false, err
	}

	return false, nil
}

// GetAppConfig returns the app configuration
func GetAppConfig(filePath string) *AppConfig {
	err := coreutils.LoadYAMLFile(filePath, &appConfig)
	if err != nil {
		appConfig = defaultAppConfig
	}
	return appConfig
}

// SaveAppConfig saves the app configuration
func SaveAppConfig(filePath string, createFile bool) error {
	err := coreutils.SaveYAMLFile(filePath, appConfig, createFile)
	if err != nil {
		return err
	}

	return nil
}
