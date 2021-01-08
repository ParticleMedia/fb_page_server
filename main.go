package main

import (
	"flag"
	"fmt"
	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/ParticleMedia/nonlocal-indexer/server"
	"github.com/golang/glog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var configFile = flag.String("conf", "../conf/fb_page_tcat.yaml", "path of config")
var handler *server.Handler = nil
var stopOnce = &sync.Once{}

func initGlobalResources() {
	confErr := common.LoadConfig(*configFile)
	if confErr != nil {
		glog.Fatalf("failed to config: %+v", confErr)
	}
	glog.Infof("Load config file: %+v success", *configFile)
}

func releaseGlobalResources() {
	if handler != nil {
		handler.Stop()
		handler = nil
		glog.Info("Handler stopped!")
	}
}

func handleSignal(c <-chan os.Signal) {
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			glog.Infof("Exit with signal: %v", s)
			stopOnce.Do(releaseGlobalResources)
			glog.Flush()
			os.Exit(0)
		}
	}
}

func main() {
	flag.Parse()
	defer func() { // 必须要先声明defer，否则不能捕获到panic异常
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容
		}
	}()
	defer glog.Flush()

	initGlobalResources()
	defer stopOnce.Do(releaseGlobalResources)

	//创建监听退出chan
	c := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go handleSignal(c)

	handler, err := server.NewHandler(common.ServiceConfig)
	if err != nil {
		glog.Fatalf("Init handler error %+v", err)
	}
	handler.Start()
	handler.Join()
}
