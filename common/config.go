package common

import (
	yaml "gopkg.in/yaml.v3"
	"os"
	"sync"
)

var FBConfig *Config
var Wg = &sync.WaitGroup{}

type LogConfig struct {
	InfoLevel  int32  `yaml:"info_level"`
	SampleRate uint32 `yaml:"sample_rate"`
}

type KafkaConfig struct {
	Addr           []string `yaml:"addrs"`
	Topic          []string `yaml:"topics"`
	GroupId        string   `yaml:"group_id"`
	OffsetInitial  int64    `yaml:"offset_initial"`
	CommitInterval int64    `yaml:"commit_interval"`
}

type MongoConfig struct {
	Addr       string   `yaml:"addr"`
	ReplicaSet string   `yaml:"replica_set"`
	Timeout    int64    `yaml:"timeout"`
	Database   string   `yaml:"database"`
	Collection []string `yaml:"collections"`
}

type TextCategoryConfig struct {
	Uri         string `yaml:"uri"`
	ContentType string `yaml:"content_type"`
	Collection  string `yaml:"collection"`
	Profile     string `yaml:"profile"`
}

type ChannelConfig struct {
	Uri         string `yaml:"uri"`
	ContentType string `yaml:"content_type"`
	Collection  string `yaml:"collection"`
	Profile     string `yaml:"profile"`
}

type UserProfileConfig struct {
	Addr         string `yaml:"addr"`
	Timeout      int64  `yaml:"timeout"`
	ReqFrom      string `yaml:"req_from"`
	Version      int64  `yaml:"version"`
	Format       string `yaml:"format"`
	DisableCache bool   `yaml:"disable_cache"`
}

type Config struct {
	WorkerCnt int                `yaml:"worker_cnt"`
	LogConf   LogConfig          `yaml:"log"`
	KafkaConf KafkaConfig        `yaml:"kafka"`
	MongoConf MongoConfig        `yaml:"mongo"`
	TcatConf  TextCategoryConfig `yaml:"text_category"`
	ChnConf   ChannelConfig      `yaml:"channel"`
	UpsConf   UserProfileConfig  `yaml:"user_profile"`
}

func LoadConfig(confPath string) error {
	filePtr, err := os.Open(confPath)
	if err != nil {
		return err
	}
	defer filePtr.Close()

	decoder := yaml.NewDecoder(filePtr)
	FBConfig = &Config{}
	err = decoder.Decode(FBConfig)
	return err
}
