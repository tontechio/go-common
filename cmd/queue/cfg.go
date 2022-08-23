package main

import (
	"fmt"

	"github.com/spf13/pflag"
)

type cfg struct {
	logger struct {
		host  string
		level string
	}
	amqp struct {
		host     string
		port     int
		username string
		password string
		vhost    string
	}
	queue struct {
		exchange   string
		name       string
		routingKey string
		limit      int
	}
}

func setCfg(f *pflag.FlagSet) {
	f.StringP("sentry", "s", "", "sentry dsn")
	f.StringP("host", "r", "localhost", "rabbitmq host")
	f.IntP("port", "p", 5672, "rabbitmq port")
	f.StringP("username", "u", "rabbitmq", "rabbitmq username")
	f.StringP("password", "w", "rabbitmq", "rabbitmq password")
	f.StringP("vhost", "v", "/", "rabbitmq vhost")
	f.StringP("exchange", "e", "default", "exchange")
	f.StringP("queue", "q", "test_error", "deferred queue name")
	f.StringP("routing_key", "k", "test", "queue name")
	f.IntP("limit", "l", 10, "limit of messages")
}

func getCfg(f *pflag.FlagSet) (*cfg, error) {
	var (
		err error
		c   = &cfg{}
	)

	if c.logger.host, err = f.GetString("sentry"); err != nil {
		return nil, fmt.Errorf("parse logger host error: %w", err)
	}

	if c.amqp.host, err = f.GetString("host"); err != nil {
		return nil, fmt.Errorf("parse amqp host error: %w", err)
	}

	if c.amqp.port, err = f.GetInt("port"); err != nil {
		return nil, fmt.Errorf("parse amqp port error: %w", err)
	}

	if c.amqp.username, err = f.GetString("username"); err != nil {
		return nil, fmt.Errorf("parse amqp username error: %w", err)
	}

	if c.amqp.password, err = f.GetString("password"); err != nil {
		return nil, fmt.Errorf("parse amqp password error: %w", err)
	}

	if c.amqp.vhost, err = f.GetString("vhost"); err != nil {
		return nil, fmt.Errorf("parse amqp vhost error: %w", err)
	}

	if c.queue.exchange, err = f.GetString("exchange"); err != nil {
		return nil, fmt.Errorf("parse queue exchange error: %w", err)
	}

	if c.queue.name, err = f.GetString("queue"); err != nil {
		return nil, fmt.Errorf("parse queue name error: %w", err)
	}

	if c.queue.routingKey, err = f.GetString("routing_key"); err != nil {
		return nil, fmt.Errorf("parse queue v error: %w", err)
	}

	if c.queue.limit, err = f.GetInt("limit"); err != nil {
		return nil, fmt.Errorf("parse queue limit error: %w", err)
	}

	return c, nil
}
