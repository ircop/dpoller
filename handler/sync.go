package handler

import (
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dproto"
)

func (h *handler) SyncObjects(dbd dproto.DBD) {
	newObjects := make(map[int64]dproto.DBObject)
	for i := range dbd.Objects {
		newObjects[dbd.Objects[i].ID] = *dbd.Objects[i]
	}

	h.processObjects(newObjects, true)
}

func (h *handler) UpdateObjects(update dproto.DBUpdate) {
	newObjects := make(map[int64]dproto.DBObject)
	for i := range update.Objects {
		newObjects[update.Objects[i].ID] = *update.Objects[i]
	}

	h.processObjects(newObjects, false)
}

func (h *handler) PingUpdate(update dproto.Pingupdate) {
	old, ok := h.Objects.Load(update.OID)
	if !ok {
		return
	}
	object := old.(*Object)
	object.MX.Lock()
	object.DBO.Alive = update.Alive
	object.MX.Unlock()
}

func (h *handler) processObjects(newObjects map[int64]dproto.DBObject, sync bool) {
	memObjects := make(map[int64]*Object)
	if sync {
		// if this is full sync, remove non-existing objects
		memObjects = h.GetAllObjects()
		for id, old := range memObjects {
			if _, ok := newObjects[id]; !ok {
				// remove object from memory
				old.MX.Lock()
				if old.Timer != nil {
					old.Timer.Stop()
					old.Timer = nil
				}
				h.Objects.Delete(id)
				old.MX.Unlock()
				logger.Debug("Deleted object #%d", id)
			}
		}
	}

	if !sync {
		for id := range newObjects {
			oInt, ok := h.Objects.Load(id)
			if ok {
				o := oInt.(*Object)
				memObjects[id] = o
			}
		}
	}

	// now compare/update objects
	for id, newObj := range newObjects {
		old, ok := memObjects[id]
		if !ok {
			// store new object =\
			obj := Object{
				DBO:newObj,
				Deleted:false,
			}
			h.Objects.Store(newObj.ID, &obj)
			obj.ShedulePoll(true)
			if newObj.Addr == "10.170.214.31" {
				logger.Debug("Stored new object #%d (%s)", newObj.ID, newObj.Addr)
			}
		} else {
			// object exist, compare
			old.MX.Lock()
			dbo := old.DBO
			old.MX.Unlock()

			if newObj.Removed {
				old.MX.Lock()
				if old.Timer != nil {
					old.Timer.Stop()
					old.Timer = nil
				}
				old.Deleted = true
				h.Objects.Delete(id)
				old.MX.Unlock()
				logger.Debug("Removed object #%d (%s)", newObj.ID, newObj.Addr)
				continue
			}

			//if dbo.Addr != newObj.Addr || dbo.PollInterval != newObj.PollInterval || dbo.RoCommunity != newObj.RoCommunity || dbo.Alive != newObj.Alive {
				old.MX.Lock()
				old.DBO = newObj
				old.MX.Unlock()
				logger.Debug("Updated object #%d (%s)", newObj.ID, newObj.Addr)

				if newObj.PollInterval < dbo.PollInterval {
					old.ShedulePoll(true)
				}
			//}
		}
	}
}
