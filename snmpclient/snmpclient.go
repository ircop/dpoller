package snmpclient

import (
	"fmt"
	"github.com/soniah/gosnmp"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type Snmpclient struct {
	RoCommunity		string
	Addr			string
	client			gosnmp.GoSNMP
	//client			*wapsnmp.WapSNMP
}

func NewClient(community string, addr string, debug bool) (*Snmpclient, error) {
	if community == "" {
		return nil, fmt.Errorf("Wrong SNMP community")
	}
	if ip := net.ParseIP(addr); ip == nil {
		return nil, fmt.Errorf("Wrong address given")
	}

	c := Snmpclient{
		RoCommunity:community,
		Addr:addr,
	}

	c.client = gosnmp.GoSNMP{
		Target:c.Addr,
		Port:gosnmp.Default.Port,
		Community:c.RoCommunity,
		Version:gosnmp.Version2c,
		Timeout:time.Duration(15) * time.Second,
		Retries:1,
		//Logger:log.New(os.Stdout, "Snmpclient: ", 0),
	}
	if debug {
		c.client.Logger = log.New(os.Stdout, "Snmpclient: ", 0)
	}
	var err error

	err = c.client.Connect()
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Snmpclient) Close() {
	c.client.Conn.Close()
}

// Snmpget expects only one result variable (or no such instance or something like that)
func (c *Snmpclient) Snmpget(oid string, expectedType gosnmp.Asn1BER) (interface{}, error) {
	result, err := c.client.Get([]string{oid})
	if err != nil {
		return nil, err
	}

	if len(result.Variables) == 0 {
		return nil, fmt.Errorf("Empty response")
	}

	item := result.Variables[0]

	if item.Type != expectedType {
		return item, fmt.Errorf("Wrong result type (%v)", item.Type)
	}

	return item.Value, nil
}

func (c *Snmpclient) WalkWithLastID(oid string, expectedType gosnmp.Asn1BER) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	results, err := c.client.BulkWalkAll(oid)
	if err != nil {
		return result, err
	}

	for i := range results {
		if results[i].Type != expectedType {
			return result, fmt.Errorf("Wrong result type (%v)", results[i].Type)
		}

		resultOID := results[i].Name
		items := strings.Split(resultOID, ".")
		if len(items) == 0 {
			return result, fmt.Errorf("Wrong result instance name '%s'", results[i].Name)
		}
		item := items[len(items)-1]

		result[item] = results[i].Value
	}

	return result, nil
}

func (c *Snmpclient) Snmpwalk(oid string) ([]gosnmp.SnmpPDU, error) {
	return c.client.BulkWalkAll(oid)
}
