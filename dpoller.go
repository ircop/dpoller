package main

/*
Concept:
- After running, poller subscribes on 1) DB channel, 2) PING channel and listens for updates.
- After running, poller request DB SYNC
- On recieving SYNC, poller stores objects ( and interfaces ) in memory
- If object has RO community and poll interval > 0, poller shedules job.
*/

import (
	"flag"
	"fmt"
	"github.com/ircop/dpoller/cfg"
	"github.com/ircop/dpoller/chwriter"
	"github.com/ircop/dpoller/handler"
	"github.com/ircop/dpoller/logger"
	"github.com/ircop/dpoller/nats"
	"github.com/ircop/dpoller/snmpclient"
	"time"
)

func main() {
	configPath := flag.String("c", "./dpoller.toml", "Config file location")
	flag.Parse()

	config, err := cfg.NewCfg(*configPath)
	if err != nil {
		fmt.Printf("[FATAL]: Cannot read config: %s\n", err.Error())
		return
	}

	// logger
	if err := logger.InitLogger(config.LogDebug, config.LogDir); err != nil {
		fmt.Printf("[FATAL]: %s\n", err.Error())
		return
	}

	// ch-writer
	interfacesChan := make(chan snmpclient.Interface)
	if err := chwriter.Init(config.ChConnstring, config.ChMaxInterval, config.ChMaxCount, interfacesChan); err != nil {
		logger.Err(err.Error())
		return
	}
	//go chwriter.Writer.Listen(interfacesChan)

	handler.Location, err = time.LoadLocation("Local")
	handler.Handler.InterfacesChan = interfacesChan
	if err != nil {
		logger.Err("Failed to load local time location: %s", err.Error())
		return
	}

	if err := nats.Init(config.NatsURL, config.NatsDB, config.NatsPing); err != nil {
		logger.Err("[FATAL]: %s", err.Error())
		return
	}

	select{}
}
