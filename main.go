package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/ParticleMedia/nonlocal-indexer/pprof"
	"github.com/ParticleMedia/nonlocal-indexer/server"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
)

const (
	defaultConfigPath = "../conf/nonlocal_indexer.yaml"
)

var configFile = flag.String("conf", defaultConfigPath, "path of config")
var metricsToTsdb = flag.Bool("metrics_to_tsdb", false, "push metrics to tsdb")
var handler *server.Handler = nil
var stopped int32 = 0
var stopOnce = &sync.Once{}

func initGlobalResources() {
	// config
	confErr := common.LoadConfig(*configFile)
	if confErr != nil {
		glog.Fatalf("failed to config: %+v", confErr)
	}
	glog.Infof("Load config file: %+v success", *configFile)
	glog.V(16).Infof("config content: %+v", *common.ServiceConfig)

	// emit metrics
	if *metricsToTsdb {
		addr, tsdbErr := net.ResolveTCPAddr("tcp", common.ServiceConfig.Tsdb.Addr)
		if tsdbErr != nil {
			glog.Fatalf("resolve tsdb address %s error: %+v", common.ServiceConfig.Tsdb.Addr, tsdbErr)
		}
		go metrics.OpenTSDB(metrics.DefaultRegistry, time.Duration(common.ServiceConfig.Tsdb.Duration)*time.Minute, common.ServiceConfig.Tsdb.Prefix, addr)
		//go metrics.Log(metrics.DefaultRegistry, 1*time.Minute, log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
	} else {
		glog.Info("do not report metrics")
	}
	metrics.GetOrRegister("goroutine.count", metrics.NewFunctionalGauge(func() int64 { return int64(runtime.NumGoroutine()) }))
}

func releaseGlobalResources() {
	atomic.StoreInt32(&stopped, 1)
	if handler != nil {
		handler.Stop()
		handler = nil
		glog.Info("Handler stopped!")
	}
	metrics.DefaultRegistry.UnregisterAll()
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

	pprof.StartPProfServer(common.ServiceConfig.PprofPort)

	handler, err := server.NewHandler(common.ServiceConfig)
	if err != nil {
		glog.Fatalf("Init handler error %+v", err)
	}
	handler.Start()
	handler.Join()
}
