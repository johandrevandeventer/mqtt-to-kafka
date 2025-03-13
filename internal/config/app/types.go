package app

// ======================== App ======================== //

type AppConfig struct {
	Runtime RuntimeConfig `mapstructure:"runtime" yaml:"runtime"`
	Logging LoggingConfig `mapstructure:"logging" yaml:"logging"`
	Mqtt    MqttConfig    `mapstructure:"mqtt" yaml:"mqtt"`
}

type RuntimeConfig struct {
	RootDir                string `mapstructure:"root_dir" yaml:"root_dir"`
	TmpDir                 string `mapstructure:"tmp_dir" yaml:"tmp_dir"`
	PersistFilePath        string `mapstructure:"persist_file_path" yaml:"persist_file_path"`
	StopFileFilepath       string `mapstructure:"stop_file_filepath" yaml:"stop_file_filepath"`
	ConnectionsLogFilePath string `mapstructure:"connections_log_file_path" yaml:"connections_log_file_path"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level" yaml:"level"`
	FilePath   string `mapstructure:"file_path" yaml:"file_path"`
	MaxSize    int    `mapstructure:"max_size" yaml:"max_size"`
	MaxBackups int    `mapstructure:"max_backups" yaml:"max_backups"`
	MaxAge     int    `mapstructure:"max_age" yaml:"max_age"`
	Compress   bool   `mapstructure:"compress" yaml:"compress"`
	AddTime    bool   `mapstructure:"add_time" yaml:"add_time"`
}

type MqttConfig struct {
	Broker             string `mapstructure:"broker" yaml:"broker"`
	ClientId           string `mapstructure:"client_id" yaml:"client_id"`
	Port               int    `mapstructure:"port" yaml:"port"`
	Topic              string `mapstructure:"topic" yaml:"topic"`
	Qos                byte   `mapstructure:"qos" yaml:"qos"`
	CleanSession       bool   `mapstructure:"clean_session" yaml:"clean_session"`
	KeepAlive          int    `mapstructure:"keep_alive" yaml:"keep_alive"`
	ReconnectOnFailure bool   `mapstructure:"reconnect_on_failure" yaml:"reconnect_on_failure"`
	Username           string `mapstructure:"username" yaml:"username"`
	Password           string `mapstructure:"password" yaml:"password"`
}
