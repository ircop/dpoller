package nats

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/ircop/dpoller/handler"
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dproto"
	nats "github.com/nats-io/go-nats-streaming"
	"github.com/sasha-s/go-deadlock"
	"os"
	"strings"
	"time"
)

type NatsClient struct {
	Conn		nats.Conn
	URL			string
	DbChan		string
	PingChan	string
}

var SendLock deadlock.Mutex
var Nats NatsClient

func Init(url string, dbChan string, pingChan string) error {
	logger.Log("Initializing NATS connection to %s and %s...", dbChan, pingChan)
	var err error

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("Cannot discover hostname")
	}
	hostname = strings.Replace(hostname, ".", "-", -1) + "-poller"

	Nats.URL = url
	Nats.DbChan = dbChan
	Nats.PingChan = pingChan

	if Nats.Conn, err = nats.Connect("test-cluster", hostname, nats.ConnectWait(time.Second * 10), nats.NatsURL(Nats.URL)); err != nil {
		return err
	}

	// subscribe to DB channel
	_, err = Nats.Conn.Subscribe(dbChan, func(msg *nats.Msg) {
		// process DB packet
		Nats.processPacket(msg)
	},
		nats.DurableName(dbChan),
		nats.MaxInflight(200),
		nats.SetManualAckMode(),
		nats.AckWait(time.Minute * 5))
	if err != nil {
		return err
	}

	// subscribe to PING channel
	_, err = Nats.Conn.Subscribe(pingChan, func(msg *nats.Msg) {
		// process PING UPDATE packet
		Nats.processPacket(msg)
	},
		nats.DurableName(dbChan),
		nats.MaxInflight(200),
		nats.SetManualAckMode(),
		nats.AckWait(time.Minute * 5))
	if err != nil {
		return err
	}

	Nats.RequestSync()

	return nil
}

func (n *NatsClient) processPacket(msg *nats.Msg) {
	defer msg.Ack()

	var packet dproto.DPacket
	err := proto.Unmarshal(msg.Data, &packet)
	if err != nil {
		logger.Err("Cannot unmarshal DPacket: %s", err.Error())
		return
	}

	if packet.PacketType == dproto.PacketType_DB {
		var dbd dproto.DBD
		if err := proto.Unmarshal(packet.Payload.Value, &dbd); err != nil {
			logger.Err("Failed to unmarshal DBD packet: %s", err.Error())
			return
		}
		logger.Debug("Got DB Sync packet")
		// process DBDescription packet
		//processDBD(dbd)
		handler.Handler.SyncObjects(dbd)
		return
	}

	if packet.PacketType == dproto.PacketType_DB_UPDATE {
		var update dproto.DBUpdate
		if err := proto.Unmarshal(packet.Payload.Value, &update); err != nil {
			logger.Err("Failed to unmarshal DBUpdate packet: %s", err.Error())
			return
		}
		logger.Debug("Got DB Update packet")
		// process DB Update packet
		//processUpdate(update)
		handler.Handler.UpdateObjects(update)
		return
	}

	if packet.PacketType == dproto.PacketType_PINGUPDATE {
		var update dproto.Pingupdate
		if err := proto.Unmarshal(packet.Payload.Value, &update); err != nil {
			logger.Err("Failed to unmarshal Pingupdate packet: %s", err.Error())
			return
		}
		logger.Debug("Got PingUpdate packet")
		handler.Handler.PingUpdate(update)
		return
	}
}

func (n *NatsClient) RequestSync() {
	packet := dproto.DPacket{
		PacketType:dproto.PacketType_DB_REQUEST,
		Payload:&any.Any{
			Value:[]byte{},
			TypeUrl:dproto.PacketType_DB_REQUEST.String(),
		},
	}

	packetBts, err := proto.Marshal(&packet)
	if err != nil {
		logger.Err("Cannot marshal DBD Request: %s", err.Error())
		return
	}

	SendLock.Lock()
	defer SendLock.Unlock()
	logger.Debug("Sending DB_REQUEST to %s: %+#v", Nats.DbChan, packetBts)
	_, err = Nats.Conn.PublishAsync(
		Nats.DbChan,
		packetBts,
		func(g string, e error) {
			if e != nil {
				logger.Err("Error recieving NATS ACK: %s", e.Error())
			}
		})
	if err != nil {
		logger.Err("Failed to send NATS DBD request: %s", err.Error())
	}
}

