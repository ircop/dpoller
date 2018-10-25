package handler

import (
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dpoller/snmpclient"
	"strings"
)

func (o *Object) Poll() {
	o.MX.Lock()
	dbo := o.DBO
	o.MX.Unlock()
	if !dbo.Alive {
		return
	}

	if dbo.Addr == "10.170.214.31" {
		logger.Debug("Poll() for %s", dbo.Addr)
	}

	client, err := snmpclient.NewClient(dbo.RoCommunity, dbo.Addr, false)
	if err != nil {
		logger.Err("Failed to initialize snmpclient on %d/%s: %s", dbo.ID, dbo.Addr, err.Error())
		return
	}
	defer client.Close()

	ifaces, err := client.GetInterfaces()
	if err != nil {
		logger.Err("Failed to get interfaces on %d/%s: %s", dbo.ID, dbo.Addr, err.Error())
		return
	}

	// todo: should assign interface id from db somehow........
	ifnames := make(map[string]int64)
	for i := range dbo.Interfaces {
		ifnames[dbo.Interfaces[i].Name] = dbo.Interfaces[i].ID
		ifnames[dbo.Interfaces[i].Shortname] = dbo.Interfaces[i].ID
	}
	if dbo.Addr == "10.170.214.31" {
		logger.Debug("%s: dbo interfaces count: %d", dbo.Addr, len(ifnames))
	}

	for i := range ifaces {
		iface := ifaces[i]
		ifid, ok := ifnames[iface.Name]
		if !ok {
			// try to parse DLink 'x/y' interfaces
			if strings.Contains(iface.Name, "/") {
				arr := strings.Split(iface.Name, "/")
				if len(arr) == 2 {
					var ok2 bool
					if ifid, ok2 = ifnames[arr[1]]; !ok2 {
						if ifid, ok2 = ifnames[arr[0]+":"+arr[1]]; !ok2 {
							continue
						}
					}
				} else {
					continue
				}
			}
			//continue
		}
		iface.ID = ifid

		//logger.Debug("DBO %s IFACE %s ID %d INOctets %d" , dbo.Addr, iface.Name, iface.ID, iface.InOctets)

		Handler.InterfacesChan <- iface
		// todo: assign ID or skip
		// todo: bulk insert here.
		// todo: send into channel
	}
}
