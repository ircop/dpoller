package cfg

import (
	"github.com/spf13/viper"
)

// Cfg is struct for handling config parameters
type Cfg struct {
	NatsURL			string
	NatsDB			string
	NatsPing		string

	LogDir			string
	LogDebug		bool

	ChConnstring	string
	ChMaxInterval	int64
	ChMaxCount		int64
}

// NewCfg reads config with given path
func NewCfg(path string) (*Cfg, error) {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	c := new(Cfg)

	c.NatsURL = viper.GetString("nats.url")
	c.NatsDB = viper.GetString("nats.db-chan")
	c.NatsPing = viper.GetString("nats.ping-chan")

	c.LogDir = viper.GetString("log.dir")
	c.LogDebug = viper.GetBool("log.debug")

	c.ChConnstring = viper.GetString("ch.connection-string")
	c.ChMaxInterval = viper.GetInt64("ch.max-interval")
	c.ChMaxCount = viper.GetInt64("ch.max-count")

	return c, nil
}
