package main

import (
	"flag"
	"github.com/ParticleMedia/fb_page_tcat/common"
	infolog "github.com/ParticleMedia/fb_page_tcat/log"
	"github.com/ParticleMedia/fb_page_tcat/server"
	"github.com/ParticleMedia/fb_page_tcat/storage"
	"github.com/golang/glog"
)

const (
	defaultConfigPath = "../conf/fb_page_tcat.yaml"
)

var configFile = flag.String("conf", defaultConfigPath, "path of config")

func InitGlobalResources() (error) {
	confErr := common.LoadConfig(*configFile)
	if confErr != nil {
		glog.Warningf("load config with error: %+v", confErr)
		return confErr
	}
	glog.Infof("load config success from file: %+v", *configFile)

	infolog.SetInfoLogLevel(glog.Level(common.FBConfig.LogConf.InfoLevel))

	server.InitClusterConfig(&common.FBConfig.KafkaConf)

	mongoErr := storage.BuildMongoClient(&common.FBConfig.MongoConf)
	if mongoErr != nil {
		glog.Warningf("build mongo client with error: %+v", mongoErr)
		return mongoErr
	}
	glog.Infof("connect success to mogodb: %s", common.FBConfig.MongoConf.Addr)

	storage.SetValueType(&common.FBConfig.UpsConf)

	common.Wg.Add(1)
	return nil
}

func ReleaseGlobalResources() {
	common.Wg.Wait()

	disConnErr := storage.MongoDisconnect()
	if disConnErr != nil {
		glog.Warningf("mongo client disconnect with error: %+v", disConnErr)
	}
}

func main()  {
	initErr := InitGlobalResources()
	if initErr != nil {
		panic(initErr)
	}

	go server.Consume()

	ReleaseGlobalResources()
}
