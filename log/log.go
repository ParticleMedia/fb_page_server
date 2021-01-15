package infolog

import (
	"encoding/json"
	"fmt"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"math/rand"
	"time"
	"github.com/golang/glog"
)

var InfoLogLevel glog.Level = 3

func SetInfoLogLevel(level glog.Level) {
	InfoLogLevel = level
}

type ProfileLogInfo struct {
	TotalCnt uint32 `json:"count,omitempty"`
	TotalSize uint32 `json:"size,omitempty"`
	HitCacheCnt uint32 `json:"hit,omitempty"`
	SetCacheCnt uint32 `json:"set,omitempty"`
}

type StorageLogInfo struct {
	TotalCnt uint32 `json:"count,omitempty"`
	TotalSize uint32 `json:"size,omitempty"`
	Cost int64 `json:"cost,omitempty"`
	ErrorMsg string `json:"err,omitempty"`
}

type InfoLogger struct {
	Logid int64
	From string
	Method string
	DisableCache uint32
	Uids []uint64
	ProfileInfo *ProfileLogInfo
	StorageInfo map[string]*StorageLogInfo
	LogInfo map[string]interface{}
	Status int64
	Message string
	StartTime time.Time
}


func (l *InfoLogger) Print() {
	randInt := uint32(rand.Intn(100))
	if l.Status == 0 && randInt > common.FBConfig.LogConf.SampleRate {
		// 打印日志抽样控制
		return;
	}

	cost := time.Since(l.StartTime).Nanoseconds() / 1000
	profileJson, _ := json.Marshal(l.ProfileInfo)
	storageJson, _ := json.Marshal(l.StorageInfo)

	var infoStr string = ""
	for k, v := range(l.LogInfo) {
		infoStr += fmt.Sprintf(" %s=%v", k, v)
	}

	glog.V(InfoLogLevel).Infof("logid=%d from=%s method=%s status=%d msg=%s cost=%d disable_cache=%d uids=%v profile=%s storage=%s%s",
		l.Logid, l.From, l.Method, l.Status, l.Message, cost, l.DisableCache, l.Uids, profileJson, storageJson, infoStr)
}
