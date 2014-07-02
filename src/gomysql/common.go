package gomysql

import (
	"runtime"
	"log"
	"io"
)

var tab8s = "        "

func catchError(err *error) {
	if pv := recover(); pv != nil {
		switch e := pv.(type) {
		case runtime.Error:
			panic(pv)
		case error:
			if e == io.EOF {
				*err = io.ErrUnexpectedEOF
			} else {
				*err = e
			}
		default:
			panic(pv)
		}
	}
}

func (my *MySQL)debug(format string, v ...interface{}) {
	if my.Debug {
        log.Printf(format, v);
    }
}
