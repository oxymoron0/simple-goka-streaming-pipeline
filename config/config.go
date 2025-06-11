package config

import (
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"gopkg.in/yaml.v2"
)

// KafkaConfig 구조체
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	SASL    struct {
		Enable    bool   `yaml:"enable"`
		User      string `yaml:"user"`
		Password  string `yaml:"password"`
		Mechanism string `yaml:"mechanism"`
	} `yaml:"sasl"`
	TLS struct {
		Enable bool `yaml:"enable"`
	} `yaml:"tls"`
}

// AppConfig 구조체 정의 - 전체 설정
type AppConfig struct {
	Kafka KafkaConfig `yaml:"kafka"`
}

// InitGlobalConfig 함수 - Sarama 설정을 초기화하고 전역 설정을 적용
func InitConfig() (*sarama.Config, error) {
	cfg, err := loadConfig("config/config.yaml")
	if err != nil {
		return nil, err
	}
	config := goka.DefaultConfig()

	// Broker 연결 설정
	config.Net.MaxOpenRequests = 1
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	// SASL 인증 정보 설정
	config.Net.SASL.Enable = cfg.Kafka.SASL.Enable
	config.Net.SASL.User = cfg.Kafka.SASL.User
	config.Net.SASL.Password = cfg.Kafka.SASL.Password
	config.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.Kafka.SASL.Mechanism)

	// TLS 활성화
	config.Net.TLS.Enable = cfg.Kafka.TLS.Enable

	// 전역 설정 적용
	goka.ReplaceGlobalConfig(config)
	return config, nil
}

// LoadConfig 함수
func loadConfig(path string) (AppConfig, error) {
	if path == "" {
		path = "./config/config.yaml"
	}

	var c AppConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, err
	}
	return c, nil
}
