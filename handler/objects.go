package handler

import (
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dpoller/snmpclient"
	"github.com/ircop/dproto"
	"github.com/sasha-s/go-deadlock"
	"runtime/debug"
	"sync"
	"time"
)

var Location *time.Location

type Object struct {
	DBO			dproto.DBObject
	Timer		*time.Timer
	MX			deadlock.Mutex
	Deleted	bool
}

type handler struct {
	Objects			sync.Map
	InterfacesChan	chan snmpclient.Interface
}
var Handler handler

func (h *handler) GetAllObjects() map[int64]*Object {
	objects := make(map[int64]*Object)

	h.Objects.Range(func(key, oInt interface{}) bool {
		obj := oInt.(*Object)

		obj.MX.Lock()
		objects[obj.DBO.ID] = obj
		obj.MX.Unlock()

		return true
	})

	return objects
}

func (o *Object) ShedulePoll(urgent bool) {
	if o.Deleted {
		return
	}
	o.MX.Lock()
	defer o.MX.Unlock()

	dbo := o.DBO

	interval := time.Duration(dbo.PollInterval) * time.Second
	if urgent {
		interval = 10 * time.Second
	}

	if dbo.Addr == "10.170.214.31" {
		logger.Debug("Sheduling poll for %d (%s) in %f secs", dbo.ID, dbo.Addr, interval.Seconds())
	}

	if o.Timer != nil {
		o.Timer.Stop()
		o.Timer = nil
	}

	o.Timer = time.AfterFunc(interval, func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Panic("Recovered in ShedulePoll AfterFunc for %d/%s: %+v\n%s", dbo.ID, dbo.Addr, r, debug.Stack())
			}

			o.ShedulePoll(false)
		}()

		o.Poll()
	})
}
