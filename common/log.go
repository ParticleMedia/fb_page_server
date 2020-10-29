package common

import (
	"fmt"
	"strings"
	"sync"
)

type LogInfo struct {
	sync.Map
}

func NewLogInfo() *LogInfo {
	return &LogInfo{
		sync.Map{},
	}
}

func (l *LogInfo) Set(key string, value interface{}) {
	l.Store(key, value)
}

func (l *LogInfo) ToString() string {
	if l == nil {
		return ""
	}
	splits := make([]string, 0)
	l.Range(func(k, v interface{}) bool {
		splits = append(splits, fmt.Sprintf("%v=%v", k, v))
		return true
	})
	return strings.Join(splits, " ")
}
