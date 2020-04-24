package common

import (
	"os"

	"github.com/golang/glog"
	yaml "gopkg.in/yaml.v3"
)

var ServiceConfig *Config

type KafkaConfig struct {
	Enable         bool     `yaml:"enable"`
	Addrs          []string `yaml:"addrs"`
	Version        string   `yaml:"version"`
	GroupId        string   `yaml:"group_id"`
	Topics         []string `yaml:"topics"`
	CommitInterval uint32   `yaml:"commit_interval"`
	Consumer       uint32   `yaml:"consumer"`
}

type ElasticSearchConfig struct {
	Enable  bool     `yaml:"enable"`
	Addr    []string `yaml:"addr"`
	Index   string   `yaml:"index"`
	Timeout int64    `yaml:"timeout"`
}

type Config struct {
	PprofPort uint32 `yaml:"pprof_port"`
	Expire    int64  `yaml:"expire"`

	Tsdb struct {
		Addr     string `yaml:"addr"`
		Duration uint32 `yaml:"duration_min"`
		Prefix   string `yaml:"prefix"`
	} `yaml:"tsdb"`

	Log struct {
		InfoLevel  int32  `yaml:"info_level"`
		SampleRate uint32 `yaml:"sample_rate"`
	} `yaml:"log"`

	NewsKafka  KafkaConfig         `yaml:"news_kafka"`
	LocalEs    ElasticSearchConfig `yaml:"elastic_search"`

	TraceConfig struct {
		Enable bool   `yaml:"enable"`
		Url    string `yaml:"url"`
		Event  string `yaml:"event"`
	} `yaml:"trace"`
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
	glog.V(16).Infof("config: %+v", *ServiceConfig)
	return err
}
