package common

import (
	"fmt"
	"strings"
)

type LogInfo map[string]interface{}

func NewLogInfo() *LogInfo {
	return &LogInfo{}
}

func (l *LogInfo) Set(key string, value interface{}) {
	(*l)[key] = value
}

func (l *LogInfo) ToString() string {
	if l == nil || len(*l) == 0 {
		return ""
	}
	splits := make([]string, 0, len(*l))
	for k, v := range *l {
		splits = append(splits, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(splits, " ")
}
