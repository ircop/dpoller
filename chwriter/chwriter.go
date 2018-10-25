package chwriter

import (
	"database/sql"
	"fmt"
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dpoller/snmpclient"
	"github.com/kshvakov/clickhouse"
	"github.com/sasha-s/go-deadlock"
	"runtime/debug"
	"time"
)

type Chwriter struct {
	ConnectString		string
	WriteMaxInterval	int64			// make write every N seconds
	WriteMaxCount		int64			// or when N records collected
	Conn				*sql.DB

	Interfaces			[]snmpclient.Interface
//	InterfacesChan		chan snmpclient.Interface
	InterfacesLock		deadlock.Mutex
	InterfaceTimer		*time.Timer
	//InsertPool			[]
}

/*
create table lnms.interfaces (
  interface_id UInt32,
  date Date,
  time DateTime,
  state Int8,
  speed UInt32,
  in_octets UInt64,
  out_octets UInt64,
  crc UInt64
) engine=MergeTree(date, (interface_id, time), 8192)
 */

var Writer Chwriter

func Init(cstring string, interval int64, cnt int64, interfacesChan chan snmpclient.Interface) error {
	if interval < 1 {
		return fmt.Errorf("Wrong max interval")
	}
	if cnt < 1 {
		return fmt.Errorf("Wrong max count")
	}

	Writer.ConnectString = cstring
	Writer.WriteMaxInterval = interval
	Writer.WriteMaxCount = cnt

	var err error
	Writer.Conn, err = sql.Open("clickhouse", Writer.ConnectString)
	if err != nil {
		return err
	}

	err = Writer.Conn.Ping()
	if err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Err("[%d] %s", exception.Code, exception.Message)
			return fmt.Errorf(exception.Message)
		} else {
			return err
		}
	}

	Writer.Interfaces = make([]snmpclient.Interface, 0)
	//Writer.InterfacesChan = make(chan snmpclient.Interface)

	logger.Debug("Connected to clickhouse...")
	Writer.resetInterfacesTimer()
	//Writer.InterfaceTimer = time.NewTimer(time.Duration(Writer.WriteMaxInterval) * time.Second)
	go Writer.Listen(interfacesChan)

	return nil
}


// Start goroutine, which will listen for INSERT packets, collect them up to WriteMaxCount OR up to WriteMaxInterval, and make insterts.
func (w *Chwriter) Listen(interfacesChan chan snmpclient.Interface) {
	// start listen for insert jobs
	for {
		select {
		case iface := <-interfacesChan:
			if iface.ID == 0 {
				break
			}

			w.InterfacesLock.Lock()
			w.Interfaces = append(w.Interfaces, iface)
			if int64(len(w.Interfaces)) >= w.WriteMaxCount {
				go w.writeData(w.Interfaces)
				w.Interfaces = make([]snmpclient.Interface, 0)
			}
			//logger.Debug("Appended data to interfaces, count is %d...", len(w.Interfaces))

			w.InterfacesLock.Unlock()
			break
		}
	}
}

func (w *Chwriter) resetInterfacesTimer() {
	logger.Debug("Called resetInterfacesTimer()")
	w.InterfacesLock.Lock()
	defer w.InterfacesLock.Unlock()

	if w.InterfaceTimer != nil {
		w.InterfaceTimer.Stop()
		w.InterfaceTimer = nil
	}

	w.InterfaceTimer = time.AfterFunc(time.Duration(w.WriteMaxInterval) * time.Second, func(){
		w.InterfacesLock.Lock()
		logger.Debug("Trying to write CH on-timer, data count: %d", len(w.Interfaces))

		if int64(len(w.Interfaces)) > 0 {
			go w.writeData(w.Interfaces)
			w.Interfaces = make([]snmpclient.Interface, 0)
			w.InterfacesLock.Unlock()
		} else {
			w.InterfacesLock.Unlock()
			w.resetInterfacesTimer()
		}
	})
}

func (w *Chwriter) writeData(interfaces []snmpclient.Interface) {
	defer func() {
		if r := recover(); r != nil {
			logger.Panic("Recovered in writeData: %v:\n %+v\n", r, debug.Stack())
		}
		w.resetInterfacesTimer()
	}()

	if len(interfaces) == 0 {
		logger.Log("Empty interfaces, skipping write...")
		return
	}

	// do write
	logger.Log("Writing %d interfaces into ch...", len(interfaces))

	tx, err := w.Conn.Begin()
	if err != nil {
		logger.Err("Cannot begin transaction: %s", err.Error())
		return
	}
	stmt, err := tx.Prepare(`INSERT INTO interfaces(interface_id, date, time, state, speed, in_octets, out_octets, crc) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		logger.Err("Cannot prepare statement: %s", err.Error())
		return
	}
	defer stmt.Close()

	// insert in loop........
	for i := range interfaces {
		if _, err := stmt.Exec(
				interfaces[i].ID,
				//time.Now().In(handler.Location),
				interfaces[i].CollectTime,
				interfaces[i].CollectTime,
				interfaces[i].State,
				interfaces[i].Speed,
				interfaces[i].InOctets,
				interfaces[i].OutOctets,
				interfaces[i].CRC,
			); err != nil {
				logger.Err("Failed to exec statement: %s", err.Error())
				return
			}
	}

	if err = tx.Commit(); err != nil {
		logger.Err("Failed to commit transaction: %s", err.Error())
		return
	}
}
