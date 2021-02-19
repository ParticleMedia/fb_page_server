package main

import (
	"flag"
	"github.com/ParticleMedia/fb_page_server/common"
	"github.com/ParticleMedia/fb_page_server/remote"
	"github.com/ParticleMedia/fb_page_server/server"
	"github.com/golang/glog"
)

const (
	defaultConfigPath = "../conf/fb_page_server.yaml"
)

var configFile = flag.String("conf", defaultConfigPath, "path of config")

func InitGlobalResources() (error) {
	confErr := common.LoadConfig(*configFile)
	if confErr != nil {
		glog.Warningf("load config with error: %+v", confErr)
		return confErr
	}
	glog.Infof("load config success from file: %+v", *configFile)

	server.InitClusterConfig(&common.FBConfig.KafkaConf)

	chnErr := remote.LoadChannels()
	if chnErr != nil {
		glog.Warningf("load channel data with error: %+v", chnErr)
		return chnErr
	}
	glog.Infof("load channel data success")

	mongoErr := remote.BuildMongoCollections(&common.FBConfig.MongoConf)
	if mongoErr != nil {
		glog.Warningf("build mongo collections with error: %+v", mongoErr)
		return mongoErr
	}
	glog.Infof("connect success to mogodb: %s", common.FBConfig.MongoConf.Addr)

	remote.SetValueType(&common.FBConfig.UpsConf)

	common.Wg.Add(1)
	return nil
}

func ReleaseGlobalResources() {
	common.Wg.Wait()

	disConnErr := remote.MongoDisconnect()
	if disConnErr != nil {
		glog.Warningf("mongo client disconnect with error: %+v", disConnErr)
	}
}

func main()  {
	flag.Parse()

	initErr := InitGlobalResources()
	if initErr != nil {
		panic(initErr)
	}

	go server.Consume()

	ReleaseGlobalResources()
}
