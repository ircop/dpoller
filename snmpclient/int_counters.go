package snmpclient

import "github.com/soniah/gosnmp"

func (c *Snmpclient) getIntCounters(ifaces map[string]Interface) (error) {

	// in-octets
	ins, err := c.WalkWithLastID(HCINOCTETS, gosnmp.Counter64)
	if err != nil {
		return err
	}
	for id, in := range ins {
		iface, ok := ifaces[id]
		if !ok {
			continue
		}
		iface.InOctets = in.(uint64)
		ifaces[id] = iface
	}

	// out-octets
	outs, err := c.WalkWithLastID(HCOUTOCTETS, gosnmp.Counter64)
	if err != nil {
		return err
	}
	for id, out := range outs {
		iface, ok := ifaces[id]
		if !ok {
			continue
		}
		iface.OutOctets = out.(uint64)
		ifaces[id] = iface
	}

	// CRC counter
	crcs, err := c.WalkWithLastID(CRC, gosnmp.Counter32)
	if err != nil {
		return nil
	}
	for id, crc := range crcs {
		iface, ok := ifaces[id]
		if !ok {
			continue
		}
		iface.CRC = uint32(crc.(uint))
		ifaces[id] = iface
	}

	return nil
}
