package common

import (
	"os"

	yaml "gopkg.in/yaml.v3"
)

var ServiceConfig *Config

type KafkaConfig struct {
	Addrs          []string `yaml:"addrs"`
	Version        string   `yaml:"version"`
	GroupId        string   `yaml:"group_id"`
	Topics         []string `yaml:"topics"`
	CommitInterval uint32   `yaml:"commit_interval"`
	Consumer       uint32   `yaml:"consumer"`
}

type Config struct {
	Kafka     KafkaConfig         `yaml:"kafka"`
}

func LoadConfig(confPath string) error {
	//打开文件
	filePtr, err := os.Open(confPath)
	if err != nil {
		return err
	}
	defer filePtr.Close()

	decoder := yaml.NewDecoder(filePtr)
	ServiceConfig = &Config{}
	err = decoder.Decode(ServiceConfig)
	if err != nil {
		return err
	}
	return err
}
