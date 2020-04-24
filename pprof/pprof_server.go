// +build pprof

package pprof

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/golang/glog"
)

func StartPProfServer(port uint32) {
	address := fmt.Sprintf("0.0.0.0:%d", port)
	go func() {
		err := http.ListenAndServe(address, nil)
		if err != nil {
			glog.Warning("start pprof server on %s with error %+v", address, err)
		}
	}()
}
