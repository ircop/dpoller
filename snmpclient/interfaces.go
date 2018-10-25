package snmpclient

import (
	"fmt"
	"github.com/soniah/gosnmp"
	"strconv"
	"strings"
	"time"
)

const (
	IFSTATE_UP = 1
	IFSTATE_DOWN = 0

	BIT = 1
	KB = 1000
	MB = 1000000
	GB = 1000000000
	TB = 1000000000000
)

type Interface struct {
	CollectTime	time.Time
	ID			int64
	Oid			string
	Name		string
	State		int
	Speed		uint32
	SpeedSTR	string
	InOctets	uint64
	OutOctets	uint64
	CRC			uint32
}

// GetInterfaces collects interface data and returns interfaces slice (or map?)
func (c *Snmpclient) GetInterfaces() (map[string]Interface, error) {
	ifaces := make(map[string]Interface)

	ifnames, err := c.WalkWithLastID(IFNAMES, gosnmp.OctetString)
	if err != nil {
		return ifaces, err
	}

	for id, name := range ifnames {
		iface := Interface{
			Oid:id,
			Name:fmt.Sprintf("%s", name),
			SpeedSTR: "-",
			CollectTime:time.Now().Local(),
		}
		ifaces[id] = iface
	}

	// collect states
	states, err := c.WalkWithLastID(IFSTATES, gosnmp.Integer)
	if err != nil {
		return ifaces, err
	}
	for id, state := range states {
		iface, ok := ifaces[id]
		if !ok {
			continue
		}
		if state.(int) == OPER_STATUS_UP {
			iface.State = IFSTATE_UP
		} else {
			iface.State = IFSTATE_DOWN
		}
		ifaces[id] = iface
	}


	// collect speeds
	speeds, err := c.WalkWithLastID(IFSPEED, gosnmp.Gauge32)
	if err != nil {
		return ifaces, err
	}
	for id, speed := range speeds {
		iface, ok := ifaces[id]
		if !ok {
			continue
		}
		iface.Speed = uint32(speed.(uint))
		iface.SpeedSTR = HumanBits(uint64(iface.Speed))
		ifaces[id] = iface
	}

	c.getIntCounters(ifaces)

	return ifaces, nil
}

func HumanBits(bits uint64) string {
	unit := ""
	value := float64(bits)

	switch {
	case bits >= TB:
		unit = "T"
		value = value / TB
	case bits >= GB:
		unit = "G"
		value = value / GB
	case bits >= MB:
		unit = "M"
		value = value / MB
	case bits >= KB:
		unit = "K"
		value = value / KB
	case bits >= BIT:
		unit = "B"
	case bits == 0:
		return ""
	}

	result := strconv.FormatFloat(value, 'f', 1, 64)
	result = strings.TrimSuffix(result, ".0")

	return result + unit
}